#!/usr/bin/env python3
"""
Unified INAProc scraping tool - VPS friendly, single-worker, resumable.

Fitur utama:
1. list        : scrape daftar paket dari https://data.inaproc.id/realisasi
2. detail      : scrape detail paket dari URL/kode paket/DB, transform nilai/tanggal, opsional update DB
3. participant : scrape peserta paket dari SPSE, opsional insert DB

Desain operasional:
- Single worker only: tidak ada concurrency.
- Operational storage: SQLite state DB untuk resume setelah crash/restart VPS.
- Export final: CSV/JSON dibuat dari SQLite state, bukan dipakai sebagai storage utama proses.
- Browser restart berkala.
- Cooldown periodik dan adaptive cooldown saat error/timeout beruntun.
- Resource blocking default: image,font,media,stylesheet.
- Debug artifact hanya dibuat saat failure.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode, urljoin

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

from playwright.sync_api import Browser, BrowserContext, Page
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


# ============================================================
# CONSTANTS
# ============================================================

INAPROC_REALISASI_BASE_URL = "https://data.inaproc.id/realisasi"
SPSE_BASE_URL = "https://spse.inaproc.id/"
DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_LONG_TIMEOUT_MS = 120_000
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

DEFAULT_BLOCK_RESOURCES = "image,font,media,stylesheet"
DEFAULT_BLOCK_URL_KEYWORDS = (
    "googletagmanager,google-analytics,analytics,doubleclick,adservice,"
    "facebook,hotjar,clarity,segment,adsystem,beacon"
)

SPSE_SEARCH_SELECTORS = [
    'input[placeholder="Cari K/L/Pemda/instansi Lainnya atau SPSE"]',
    'input[placeholder*="Cari K/L/Pemda"]',
    'input[placeholder*="SPSE"]',
    'input[type="search"]',
]

DEFAULT_DETAIL_DB_FIELD_MAP = {
    "Cara Pembayaran": "cara_pembayaran",
    "Instansi": "instansi",
    "Kualifikasi Usaha": "kualifikasi_usaha",
    "Lokasi Pekerjaan": "lokasi_pekerjaan",
    "Metode Evaluasi": "metode_evaluasi",
    "Nilai HPS": "nilai_hps",
    "Nilai Pagu": "nilai_pagu",
    "Satuan Kerja": "satuan_kerja",
    # Field transform dari "Tanggal Tender"
    "tanggal_tender_mulai": "tanggal_tender_mulai",
    "tanggal_tender_selesai": "tanggal_tender_selesai",
    "durasi_tender": "durasi_tender",
    # Aktifkan jika raw text tanggal tender tetap perlu masuk DB.
    "Tanggal Tender": "tanggal_tender",
}

MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "januari": 1,
    "feb": 2,
    "february": 2,
    "februari": 2,
    "mar": 3,
    "march": 3,
    "maret": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "mei": 5,
    "jun": 6,
    "june": 6,
    "juni": 6,
    "jul": 7,
    "july": 7,
    "juli": 7,
    "aug": 8,
    "agu": 8,
    "agustus": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "okt": 10,
    "october": 10,
    "oktober": 10,
    "nov": 11,
    "nop": 11,
    "november": 11,
    "dec": 12,
    "des": 12,
    "december": 12,
    "desember": 12,
}


# ============================================================
# BOOTSTRAP: .env, PATHS, LOGGING
# ============================================================

def load_dotenv(dotenv_path: Path = Path(".env")) -> None:
    if not dotenv_path.exists():
        return

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_dotenv()

Path("logs").mkdir(exist_ok=True)
Path("debug/screenshots").mkdir(parents=True, exist_ok=True)
Path("debug/html").mkdir(parents=True, exist_ok=True)
Path("debug/traces").mkdir(parents=True, exist_ok=True)
Path("output").mkdir(exist_ok=True)
Path("state").mkdir(exist_ok=True)

logging.basicConfig(
    filename=f"logs/inaproc_tool_{RUN_ID}.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("inaproc_tool")


# ============================================================
# COMMON HELPERS
# ============================================================

def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_header(value: Any) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_key(value: Any) -> str:
    return clean_text(value).lower()


def make_unique_key(existing_keys: set[str], key: str) -> str:
    base_key = clean_text(key) or "Unnamed"
    final_key = base_key
    counter = 2
    while final_key in existing_keys:
        final_key = f"{base_key}_{counter}"
        counter += 1
    existing_keys.add(final_key)
    return final_key


def safe_filename(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-zA-Z0-9_-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:100] or "unknown"


def unique_headers(headers: list[str], width: int) -> list[str]:
    if not headers:
        headers = [f"column_{index + 1}" for index in range(width)]

    result: list[str] = []
    used: dict[str, int] = {}

    for index in range(width):
        header = clean_text(headers[index] if index < len(headers) else "")
        if not header:
            header = f"column_{index + 1}"
        used[header] = used.get(header, 0) + 1
        result.append(header if used[header] == 1 else f"{header}_{used[header]}")

    return result


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise ValueError(f"CSV tidak memiliki header: {path}")
        return [dict(row) for row in reader]


def write_records(path: Path, records: list[dict[str, Any]], output_format: str = "csv") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "json":
        path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def append_records_csv(path: Path, records: list[dict[str, Any]]) -> None:
    """Dipakai hanya untuk failed CSV; output final tetap diekspor dari SQLite state."""
    if not records:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows: list[dict[str, Any]] = []
    if path.exists() and path.stat().st_size > 0:
        try:
            existing_rows = read_csv_rows(path)
        except Exception as exc:
            logger.warning("Gagal membaca CSV lama untuk append, file akan dibuat ulang: %s", exc)
            existing_rows = []

    write_records(path, existing_rows + records, output_format="csv")


def apply_start_limit(items: list[dict[str, Any]], start_row: int | None, limit: int | None) -> list[dict[str, Any]]:
    if start_row is not None and start_row < 1:
        raise ValueError("--start-row harus bernilai minimal 1 jika diisi.")
    if limit is not None and limit < 1:
        raise ValueError("--limit harus bernilai minimal 1 jika diisi.")

    start_index = (start_row - 1) if start_row is not None else 0
    if start_index >= len(items):
        raise ValueError(f"--start-row {start_row} melebihi jumlah data: {len(items)}")
    return items[start_index:] if limit is None else items[start_index:start_index + limit]


def auto_find_column(fieldnames: Iterable[str], candidates: list[str]) -> str | None:
    fields = list(fieldnames)
    normalized = {normalize_key(field): field for field in fields}
    for candidate in candidates:
        candidate_key = normalize_key(candidate)
        if candidate_key in normalized:
            return normalized[candidate_key]
    for field in fields:
        field_key = normalize_key(field)
        for candidate in candidates:
            if normalize_key(candidate) in field_key:
                return field
    return None


def maybe_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def random_cooldown(min_seconds: float, max_seconds: float, reason: str) -> None:
    if max_seconds <= 0:
        return
    if min_seconds < 0:
        min_seconds = 0
    if max_seconds < min_seconds:
        max_seconds = min_seconds
    seconds = random.uniform(min_seconds, max_seconds)
    print(f"Cooldown {seconds:.1f}s ({reason})")
    logger.warning("COOLDOWN reason='%s' seconds=%.2f", reason, seconds)
    time.sleep(seconds)


@dataclass
class FailureBackoff:
    threshold: int
    base_seconds: float
    max_seconds: float
    consecutive_errors: int = 0

    def record_success(self) -> None:
        self.consecutive_errors = 0

    def record_failure(self) -> None:
        self.consecutive_errors += 1
        if self.consecutive_errors >= self.threshold:
            seconds = min(self.max_seconds, self.base_seconds * self.consecutive_errors)
            random_cooldown(seconds, seconds + 3, f"adaptive_error_{self.consecutive_errors}")


# ============================================================
# TRANSFORM DETAIL DATA
# ============================================================

def cleanse_rupiah_to_int(value: Any) -> int | None:
    text = clean_text(value)
    if not text:
        return None

    text = re.sub(r"(?i)\brp\b", "", text).strip()
    if "," in text:
        text = text.split(",", 1)[0]

    digits = re.sub(r"\D+", "", text)
    if not digits:
        return None
    return int(digits)


def parse_tender_date(value: Any) -> date | None:
    text = clean_text(value)
    match = re.search(
        r"(\d{1,2})\s+([A-Za-zÀ-ÿ.]+)\s+(\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    day = int(match.group(1))
    month_text = match.group(2).lower().replace(".", "")
    year = int(match.group(3))
    month = MONTH_MAP.get(month_text)
    if month is None:
        return None

    try:
        return date(year, month, day)
    except ValueError:
        return None


def split_tanggal_tender(value: Any) -> tuple[str | None, str | None, int | None]:
    text = clean_text(value)
    if not text:
        return None, None, None

    parts = re.split(
        r"\s+(?:s/d|sd|sampai|sampai dengan|-|–|—)\s+",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )
    if len(parts) != 2:
        return None, None, None

    start_date = parse_tender_date(parts[0])
    end_date = parse_tender_date(parts[1])
    if not start_date or not end_date:
        return None, None, None

    return start_date.isoformat(), end_date.isoformat(), (end_date - start_date).days


def transform_detail_scraped_data(scraped_data: dict[str, Any]) -> dict[str, Any]:
    """Transform hasil scrape detail tender sebelum disimpan ke SQLite/export/DB."""
    if "Nilai HPS" in scraped_data:
        scraped_data["Nilai HPS"] = cleanse_rupiah_to_int(scraped_data.get("Nilai HPS"))

    if "Nilai Pagu" in scraped_data:
        scraped_data["Nilai Pagu"] = cleanse_rupiah_to_int(scraped_data.get("Nilai Pagu"))

    tanggal_tender_raw = scraped_data.get("Tanggal Tender")
    if tanggal_tender_raw:
        tanggal_mulai, tanggal_selesai, durasi_tender = split_tanggal_tender(tanggal_tender_raw)
        scraped_data["tanggal_tender"] = tanggal_tender_raw  # Simpan raw text jika masih diperlukan
        scraped_data["tanggal_tender_mulai"] = tanggal_mulai
        scraped_data["tanggal_tender_selesai"] = tanggal_selesai
        scraped_data["durasi_tender"] = durasi_tender

    return scraped_data


# ============================================================
# OPERATIONAL STORAGE: SQLITE STATE
# ============================================================

class OperationalStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scrape_items (
                mode TEXT NOT NULL,
                item_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                source_row_id TEXT,
                kode_paket TEXT,
                nama_instansi TEXT,
                url TEXT,
                payload_json TEXT,
                result_json TEXT,
                last_error TEXT,
                failed_step TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (mode, item_key)
            )
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_scrape_items_mode_status
            ON scrape_items (mode, status)
            """
        )
        self.conn.commit()

    def upsert_items(self, mode: str, items: list[dict[str, Any]], reset_failed: bool = False) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        for item in items:
            item_key = make_item_key(mode, item)
            existing = self.conn.execute(
                "SELECT status FROM scrape_items WHERE mode = ? AND item_key = ?",
                (mode, item_key),
            ).fetchone()

            if existing is None:
                self.conn.execute(
                    """
                    INSERT INTO scrape_items (
                        mode, item_key, status, attempts, source_row_id,
                        kode_paket, nama_instansi, url, payload_json,
                        created_at, updated_at
                    ) VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mode,
                        item_key,
                        clean_text(item.get("source_row_id", "")),
                        clean_text(item.get("kode_paket", item.get("Kode Paket", ""))),
                        clean_text(item.get("nama_instansi", item.get("Nama Instansi", ""))),
                        clean_text(item.get("url", "")),
                        json.dumps(item, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
            elif reset_failed and existing["status"] == "failed":
                self.conn.execute(
                    """
                    UPDATE scrape_items
                    SET status = 'pending', last_error = '', failed_step = '', updated_at = ?
                    WHERE mode = ? AND item_key = ?
                    """,
                    (now, mode, item_key),
                )

        self.conn.commit()

    def pending_items(self, mode: str, retry_failed: bool = True) -> list[sqlite3.Row]:
        if retry_failed:
            statuses = ("pending", "failed")
        else:
            statuses = ("pending",)
        placeholders = ",".join("?" for _ in statuses)
        rows = self.conn.execute(
            f"""
            SELECT * FROM scrape_items
            WHERE mode = ? AND status IN ({placeholders})
            ORDER BY CAST(COALESCE(NULLIF(source_row_id, ''), '0') AS INTEGER), item_key
            """,
            (mode, *statuses),
        ).fetchall()
        return rows

    def count_by_status(self, mode: str) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS total FROM scrape_items WHERE mode = ? GROUP BY status",
            (mode,),
        ).fetchall()
        return {row["status"]: int(row["total"]) for row in rows}

    def mark_success(self, mode: str, item_key: str, result: Any) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            """
            UPDATE scrape_items
            SET status = 'success', attempts = attempts + 1,
                result_json = ?, last_error = '', failed_step = '', updated_at = ?
            WHERE mode = ? AND item_key = ?
            """,
            (json.dumps(result, ensure_ascii=False), now, mode, item_key),
        )
        self.conn.commit()

    def mark_failed(self, mode: str, item_key: str, error: Any, failed_step: str = "") -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            """
            UPDATE scrape_items
            SET status = 'failed', attempts = attempts + 1,
                last_error = ?, failed_step = ?, updated_at = ?
            WHERE mode = ? AND item_key = ?
            """,
            (str(error), failed_step, now, mode, item_key),
        )
        self.conn.commit()

    def success_results(self, mode: str) -> list[Any]:
        rows = self.conn.execute(
            """
            SELECT result_json FROM scrape_items
            WHERE mode = ? AND status = 'success' AND result_json IS NOT NULL AND result_json != ''
            ORDER BY CAST(COALESCE(NULLIF(source_row_id, ''), '0') AS INTEGER), item_key
            """,
            (mode,),
        ).fetchall()
        results: list[Any] = []
        for row in rows:
            try:
                results.append(json.loads(row["result_json"]))
            except json.JSONDecodeError:
                continue
        return results

    def failed_rows(self, mode: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM scrape_items
            WHERE mode = ? AND status = 'failed'
            ORDER BY CAST(COALESCE(NULLIF(source_row_id, ''), '0') AS INTEGER), item_key
            """,
            (mode,),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "source_row_id": row["source_row_id"],
                    "kode_paket": row["kode_paket"],
                    "nama_instansi": row["nama_instansi"],
                    "url": row["url"],
                    "attempts": row["attempts"],
                    "failed_step": row["failed_step"],
                    "last_error": row["last_error"],
                    "updated_at": row["updated_at"],
                }
            )
        return result


def row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        payload = {}
    return payload


def make_item_key(mode: str, item: dict[str, Any]) -> str:
    if mode == "list":
        page_number = item.get("page_number")
        return f"page:{page_number}"
    if mode == "detail":
        kode = clean_text(item.get("kode_paket", item.get("Kode Paket", "")))
        url = clean_text(item.get("url", ""))
        return f"kode:{kode}" if kode else f"url:{url}"
    if mode == "participant":
        kode = clean_text(item.get("kode_paket", item.get("Kode Paket", "")))
        instansi = clean_text(item.get("nama_instansi", item.get("Nama Instansi", "")))
        return f"{normalize_key(instansi)}::{kode}"
    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def export_mode_results(store: OperationalStore, mode: str, output: Path, output_format: str = "csv") -> int:
    raw_results = store.success_results(mode)
    flattened: list[dict[str, Any]] = []

    for result in raw_results:
        if isinstance(result, list):
            flattened.extend([r for r in result if isinstance(r, dict)])
        elif isinstance(result, dict):
            flattened.append(result)

    write_records(output, flattened, output_format=output_format)
    return len(flattened)


def export_failed(store: OperationalStore, mode: str, failed_output: Path | None) -> int:
    if failed_output is None:
        return 0
    rows = store.failed_rows(mode)
    write_records(failed_output, rows, output_format="csv")
    return len(rows)


# ============================================================
# DB CONFIG + HELPERS
# ============================================================

@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str = "tender"
    detail_table: str = "details"
    participant_table: str = "participant"
    key_field: str = "kode_paket"

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "DbConfig":
        return cls(
            host=args.db_host or os.getenv("INAPROC_DB_HOST", "localhost"),
            port=int(args.db_port or os.getenv("INAPROC_DB_PORT", "5432")),
            dbname=args.db_name or os.getenv("INAPROC_DB_NAME", "inaproc"),
            user=args.db_user or os.getenv("INAPROC_DB_USER", ""),
            password=args.db_password or os.getenv("INAPROC_DB_PASSWORD", ""),
            schema=args.db_schema or os.getenv("INAPROC_DB_SCHEMA", "tender"),
            detail_table=args.db_detail_table or os.getenv("INAPROC_DB_DETAIL_TABLE", "details"),
            participant_table=args.db_participant_table or os.getenv("INAPROC_DB_PARTICIPANT_TABLE", "participant"),
            key_field=args.db_key_field or os.getenv("INAPROC_DB_KEY_FIELD", "kode_paket"),
        )

    def validate(self) -> None:
        missing = []
        if not self.user:
            missing.append("db_user / INAPROC_DB_USER")
        if not self.password:
            missing.append("db_password / INAPROC_DB_PASSWORD")
        if not self.dbname:
            missing.append("db_name / INAPROC_DB_NAME")
        if missing:
            raise ValueError("Konfigurasi DB belum lengkap: " + ", ".join(missing))


def require_psycopg2() -> None:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 belum ter-install. Install dengan: pip install psycopg2-binary")


def get_db_connection(config: DbConfig):
    require_psycopg2()
    config.validate()
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )


def quoted_ident(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Nama identifier DB tidak valid: {value}")
    return value


def fetch_kode_paket_from_db(
    conn,
    config: DbConfig,
    year: str,
    nama_instansi: str,
    start: int = 0,
    limit: int | None = None,
) -> list[str]:
    schema = quoted_ident(config.schema)
    table = quoted_ident(config.detail_table)
    key = quoted_ident(config.key_field)

    query = f"""
        SELECT {key}
        FROM {schema}.{table}
        WHERE tahun_anggaran = %s
          AND nama_instansi = %s
          AND {key} IS NOT NULL
        ORDER BY {key}
    """
    params: list[Any] = [year, nama_instansi]

    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, start])
    elif start > 0:
        query += " OFFSET %s"
        params.append(start)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [str(row[config.key_field]).strip() for row in rows if row.get(config.key_field)]


def load_work_items_from_db(
    conn,
    config: DbConfig,
    year: str,
    nama_instansi: str,
    start: int = 0,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    kode_paket_list = fetch_kode_paket_from_db(
        conn=conn,
        config=config,
        year=year,
        nama_instansi=nama_instansi,
        start=start,
        limit=limit,
    )
    return [
        {"source_row_id": idx, "nama_instansi": nama_instansi, "kode_paket": kode_paket}
        for idx, kode_paket in enumerate(kode_paket_list, start=1 + start)
    ]

def fetch_detail_items_missing_instansi_from_db(
    conn,
    config: DbConfig,
    start_row: int | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Ambil kode_paket dari tender.details untuk scraping detail.

    Rule:
    - Tidak pakai filter nama_instansi.
    - Tidak pakai filter tahun_anggaran.
    - Ambil hanya row yang field instansi masih NULL atau kosong.
    - start_row basis 1, contoh:
      --start 1 berarti row pertama.
      --start 101 berarti mulai dari row ke-101.
    """
    schema = quoted_ident(config.schema)
    table = quoted_ident(config.detail_table)
    key = quoted_ident(config.key_field)

    start_index = 0
    if start_row is not None:
        if start_row < 1:
            raise ValueError("--start / --start-row harus bernilai minimal 1.")
        start_index = start_row - 1

    if limit is not None and limit < 1:
        raise ValueError("--limit harus bernilai minimal 1 jika diisi.")

    query = f"""
        SELECT
            {key},
            ROW_NUMBER() OVER (ORDER BY {key}) AS source_row_id
        FROM {schema}.{table}
        WHERE {key} IS NOT NULL
          AND NULLIF(TRIM(COALESCE(instansi::text, '')), '') IS NULL
        ORDER BY {key}
    """

    params: list[Any] = []

    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, start_index])
    elif start_index > 0:
        query += " OFFSET %s"
        params.append(start_index)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    items: list[dict[str, Any]] = []

    for fallback_idx, row in enumerate(rows, start=start_index + 1):
        kode_paket = clean_text(row.get(config.key_field))

        if not kode_paket:
            continue

        source_row_id = row.get("source_row_id") or fallback_idx

        items.append(
            {
                "source_row_id": int(source_row_id),
                "kode_paket": kode_paket,
                "url": detail_url_from_kode(kode_paket),
                "raw": {
                    "source": "db_missing_instansi",
                    "source_row_id": int(source_row_id),
                    "kode_paket": kode_paket,
                },
            }
        )

    return items


def build_update_payload(scraped_data: dict[str, Any], field_map: dict[str, str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for scrape_label, db_field in field_map.items():
        if scrape_label in scraped_data:
            payload[db_field] = scraped_data.get(scrape_label)
    return payload


def update_detail_row(
    conn,
    config: DbConfig,
    kode_paket: str,
    payload: dict[str, Any],
) -> tuple[bool, str, int]:
    """
    Update hasil scraping detail berdasarkan kode_paket saja.

    Catatan:
    - Tidak memakai nama_instansi.
    - Tidak memakai tahun_anggaran.
    - Cocok dengan flow baru: ambil target dari DB berdasarkan instansi IS NULL.
    """
    if not payload:
        return False, "Tidak ada field hasil scrape yang cocok dengan mapping database.", 0

    schema = quoted_ident(config.schema)
    table = quoted_ident(config.detail_table)
    key = quoted_ident(config.key_field)

    set_clauses: list[str] = []
    values: list[Any] = []

    for db_field, value in payload.items():
        set_clauses.append(f"{quoted_ident(db_field)} = %s")
        values.append(value)

    values.append(kode_paket)

    query = f"""
        UPDATE {schema}.{table}
        SET {", ".join(set_clauses)}
        WHERE {key} = %s
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, values)
            affected_rows = cursor.rowcount

        conn.commit()

        if affected_rows == 0:
            return False, "Tidak ada row database yang ter-update.", affected_rows

        return True, "", affected_rows

    except Exception as exc:
        conn.rollback()
        return False, str(exc), 0


def get_value_by_header(row: dict[str, Any], candidates: list[str]) -> str:
    normalized_row = {normalize_key(key): value for key, value in row.items()}
    normalized_candidates = [normalize_key(candidate) for candidate in candidates]

    for candidate in normalized_candidates:
        if candidate in normalized_row:
            return clean_text(normalized_row[candidate])

    for key, value in normalized_row.items():
        for candidate in normalized_candidates:
            if candidate in key:
                return clean_text(value)

    return ""


def normalize_participant_row_for_db(row: dict[str, Any]) -> dict[str, str]:
    return {
        "nama_peserta": get_value_by_header(row, ["Nama Peserta", "Peserta", "Nama Penyedia"]),
        "npwp": get_value_by_header(row, ["NPWP", "Npwp", "npwp"]),
        "harga_penawaran": get_value_by_header(row, ["Harga Penawaran", "Harga Penawaaran", "Penawaran"]),
        "harga_terkoreksi": get_value_by_header(row, ["Harga Terkoreksi", "Terkoreksi"]),
        "kode_paket": clean_text(row.get("Kode Paket", row.get("kode_paket", ""))),
    }


def insert_participants_to_db(
    conn,
    config: DbConfig,
    participants: list[dict[str, str]],
) -> tuple[int, int, list[dict[str, Any]]]:
    schema = quoted_ident(config.schema)
    table = quoted_ident(config.participant_table)

    insert_sql = f"""
        INSERT INTO {schema}.{table} (
            nama_peserta,
            npwp,
            harga_penawaran,
            harga_terkoreksi,
            kode_paket
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    inserted_count = 0
    failed_insert_count = 0
    failed_inserts: list[dict[str, Any]] = []

    for idx, participant in enumerate(participants, start=1):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (
                        participant.get("nama_peserta", ""),
                        participant.get("npwp", ""),
                        participant.get("harga_penawaran", ""),
                        participant.get("harga_terkoreksi", ""),
                        participant.get("kode_paket", ""),
                    ),
                )
            conn.commit()
            inserted_count += 1
        except Exception as exc:
            conn.rollback()
            failed_insert_count += 1
            failed_inserts.append({**participant, "participant_index": idx, "error_message": str(exc)})
            logger.exception("DB_INSERT_FAILED kode_paket='%s' index=%s", participant.get("kode_paket"), idx)

    return inserted_count, failed_insert_count, failed_inserts


# ============================================================
# BROWSER MANAGEMENT: LOW RESOURCE + BLOCK RESOURCE
# ============================================================

@dataclass
class BrowserSettings:
    headless: bool
    timeout: int
    block_resources: set[str]
    block_url_keywords: list[str]
    viewport_width: int = 1366
    viewport_height: int = 768


def parse_csv_set(value: str | None) -> set[str]:
    return {item.strip().lower() for item in clean_text(value).split(",") if item.strip()}


def parse_csv_list(value: str | None) -> list[str]:
    return [item.strip().lower() for item in clean_text(value).split(",") if item.strip()]


def low_resource_chromium_args() -> list[str]:
    return [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-background-networking",
        "--disable-background-timer-throttling",
        "--disable-client-side-phishing-detection",
        "--disable-default-apps",
        "--disable-extensions",
        "--disable-features=site-per-process,TranslateUI,BlinkGenPropertyTrees",
        "--disable-hang-monitor",
        "--disable-popup-blocking",
        "--disable-prompt-on-repost",
        "--disable-renderer-backgrounding",
        "--disable-sync",
        "--disable-blink-features=AutomationControlled",
        "--metrics-recording-only",
        "--mute-audio",
        "--no-first-run",
        "--no-default-browser-check",
        "--password-store=basic",
        "--use-mock-keychain",
        "--disable-gpu",
    ]


def setup_resource_blocking(context: BrowserContext, block_resources: set[str], block_url_keywords: list[str]) -> None:
    if not block_resources and not block_url_keywords:
        return

    def handler(route):
        request = route.request
        resource_type = request.resource_type.lower()
        url = request.url.lower()

        if resource_type in block_resources:
            route.abort()
            return

        if any(keyword and keyword in url for keyword in block_url_keywords):
            route.abort()
            return

        route.continue_()

    context.route("**/*", handler)


class BrowserSession:
    def __init__(self, playwright, settings: BrowserSettings):
        self.playwright = playwright
        self.settings = settings
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.restart_count = 0
        self.start()

    def start(self) -> None:
        self.browser = self.playwright.chromium.launch(
            headless=self.settings.headless,
            args=low_resource_chromium_args(),
        )
        self.context = self.browser.new_context(
            viewport={"width": self.settings.viewport_width, "height": self.settings.viewport_height},
            accept_downloads=False,
            locale="id-ID",
            timezone_id="Asia/Jakarta",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        setup_resource_blocking(
            self.context,
            self.settings.block_resources,
            self.settings.block_url_keywords,
        )
        self.context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """
        )

    def restart(self, reason: str) -> None:
        logger.warning("BROWSER_RESTART reason='%s' count=%s", reason, self.restart_count + 1)
        print(f"Restart Chromium ({reason})")
        self.close()
        self.restart_count += 1
        self.start()

    def new_page(self) -> Page:
        if self.context is None:
            raise RuntimeError("Browser context belum tersedia.")
        page = self.context.new_page()
        page.set_default_timeout(self.settings.timeout)
        return page

    def close(self) -> None:
        try:
            if self.context is not None:
                self.context.close()
        except Exception:
            pass
        try:
            if self.browser is not None:
                self.browser.close()
        except Exception:
            pass
        self.context = None
        self.browser = None


def build_browser_settings(args: argparse.Namespace, width: int = 1366, height: int = 768) -> BrowserSettings:
    block_resources = parse_csv_set(
        args.block_resources or os.getenv("INAPROC_BLOCK_RESOURCES", DEFAULT_BLOCK_RESOURCES)
    )
    block_url_keywords = parse_csv_list(
        args.block_url_keywords or os.getenv("INAPROC_BLOCK_URL_KEYWORDS", DEFAULT_BLOCK_URL_KEYWORDS)
    )
    return BrowserSettings(
        headless=args.headless,
        timeout=args.timeout,
        block_resources=block_resources,
        block_url_keywords=block_url_keywords,
        viewport_width=width,
        viewport_height=height,
    )


def save_debug(page: Page, mode: str, item_key: str, step_name: str) -> None:
    debug_name = f"{safe_filename(mode)}_{safe_filename(item_key)}_{safe_filename(step_name)}"
    try:
        page.screenshot(path=f"debug/screenshots/{debug_name}.png", full_page=True)
    except Exception as exc:
        logger.warning("Failed screenshot mode=%s item=%s step=%s: %s", mode, item_key, step_name, exc)
    try:
        html = page.content()
        Path(f"debug/html/{debug_name}.html").write_text(html, encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed save HTML mode=%s item=%s step=%s: %s", mode, item_key, step_name, exc)


def run_step(step_name: str, func: Callable[[], Any]) -> Any:
    try:
        return func()
    except Exception:
        logger.exception("FAIL_STEP step=%s", step_name)
        raise


# ============================================================
# LIST SCRAPER
# ============================================================

def build_realisasi_url(year: int) -> str:
    params = [
        ("tahun", str(year)),
        ("jenis_klpd", "3"),
        ("jenis_klpd", "4"),
        ("jenis_klpd", "5"),
        ("sumber", "Tender"),
    ]
    return f"{INAPROC_REALISASI_BASE_URL}?{urlencode(params)}"


def wait_for_dataframe_table(page: Page, timeout: int = DEFAULT_LONG_TIMEOUT_MS) -> None:
    page.wait_for_selector("table.dataframe tbody tr", timeout=timeout)


def extract_dataframe_table(page: Page) -> list[dict[str, Any]]:
    table = page.locator("table.dataframe").first
    table.wait_for(state="attached", timeout=60_000)

    payload = table.evaluate(
        """
        (table) => {
            const headers = Array.from(table.querySelectorAll("thead th"))
                .map((cell) => cell.innerText.trim());
            const rows = Array.from(table.querySelectorAll("tbody tr")).map((row) =>
                Array.from(row.querySelectorAll("th, td")).map((cell) => {
                    const link = cell.querySelector("a[href]");
                    return { text: cell.innerText.trim(), href: link ? link.href : null };
                })
            );
            return { headers, rows };
        }
        """
    )

    rows = payload["rows"]
    if not rows:
        return []

    width = max(len(row) for row in rows)
    headers = unique_headers(payload["headers"], width)
    records: list[dict[str, Any]] = []

    for row in rows:
        record: dict[str, Any] = {}
        for index, header in enumerate(headers):
            cell = row[index] if index < len(row) else {"text": "", "href": None}
            record[header] = clean_text(cell.get("text"))
            if cell.get("href"):
                record[f"{header}_url"] = cell["href"]
        records.append(record)

    return records


def get_page_label(page: Page) -> str:
    labels = page.locator(r"text=/Halaman\s+\d+\s+dari\s+\d+/i")
    if labels.count() == 0:
        return ""
    return clean_text(labels.first.inner_text(timeout=2_000))


def get_visible_row_count(page: Page) -> int:
    return page.locator("table.dataframe tbody tr").count()


def set_entries_per_page(page: Page, entries_per_page: int) -> None:
    if entries_per_page <= 0:
        return

    selector = 'input[aria-label*="Entri per halaman"]'
    current_select = page.locator(selector).first
    current_select.wait_for(state="attached", timeout=60_000)
    current_label = current_select.get_attribute("aria-label") or ""

    if re.search(rf"\bSelected\s+{entries_per_page}\b", current_label, re.I):
        return

    previous_row_count = get_visible_row_count(page)
    current_select.click()
    option = page.get_by_role("option", name=str(entries_per_page), exact=True)
    option.first.click(timeout=30_000)

    try:
        page.wait_for_function(
            r"""
            ([targetRows, oldRowCount]) => {
                const select = document.querySelector('input[aria-label*="Entri per halaman"]');
                const selected = select ? select.getAttribute("aria-label") || "" : "";
                const rowCount = document.querySelectorAll("table.dataframe tbody tr").length;
                return selected.includes(`Selected ${targetRows}`)
                    && (rowCount >= targetRows || rowCount !== oldRowCount);
            }
            """,
            arg=[entries_per_page, previous_row_count],
            timeout=60_000,
        )
    except PlaywrightTimeoutError:
        page.wait_for_timeout(2_000)


def click_next_dataframe_page(page: Page) -> bool:
    next_button = page.get_by_role("button", name=re.compile("Berikutnya", re.I))
    if next_button.count() == 0:
        return False

    button = next_button.first
    if button.is_disabled():
        return False

    previous_label = get_page_label(page)
    previous_first_row = ""
    first_cell = page.locator("table.dataframe tbody tr:first-child td:first-child")
    if first_cell.count() > 0:
        previous_first_row = clean_text(first_cell.first.inner_text(timeout=2_000))

    button.click()

    try:
        page.wait_for_function(
            r"""
            ([oldLabel, oldFirstRow]) => {
                const label = Array.from(document.querySelectorAll("p"))
                    .map((node) => node.innerText.trim())
                    .find((text) => /Halaman\s+\d+\s+dari\s+\d+/i.test(text)) || "";
                const firstCell = document.querySelector("table.dataframe tbody tr:first-child td:first-child");
                const firstRow = firstCell ? firstCell.innerText.trim() : "";
                return (label && label !== oldLabel) || (firstRow && firstRow !== oldFirstRow);
            }
            """,
            arg=[previous_label, previous_first_row],
            timeout=60_000,
        )
    except PlaywrightTimeoutError:
        page.wait_for_timeout(2_000)

    return True


def open_list_page(session: BrowserSession, url: str, entries_per_page: int, target_page: int) -> Page:
    page = session.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_LONG_TIMEOUT_MS)
    wait_for_dataframe_table(page, timeout=DEFAULT_LONG_TIMEOUT_MS)
    set_entries_per_page(page, entries_per_page)

    current_page = 1
    while current_page < target_page:
        if not click_next_dataframe_page(page):
            raise RuntimeError(f"Tidak bisa menuju page {target_page}. Stop di page {current_page}.")
        current_page += 1
    return page


def build_list_work_items(start_page: int, end_page: int) -> list[dict[str, Any]]:
    return [{"source_row_id": page_number, "page_number": page_number} for page_number in range(start_page, end_page + 1)]


def scrape_list_command(args: argparse.Namespace) -> int:
    if args.start_page < 1:
        raise ValueError("--start-page harus minimal 1.")
    if args.end_page < args.start_page:
        raise ValueError("--end-page harus lebih besar atau sama dengan --start-page.")

    mode = "list"
    url = args.url or build_realisasi_url(args.year)
    store = OperationalStore(Path(args.state_db))

    try:
        if args.export_only:
            total = export_mode_results(store, mode, Path(args.output), args.format)
            print(f"Exported {total} rows to {args.output}")
            return 0

        work_items = build_list_work_items(args.start_page, args.end_page)
        store.upsert_items(mode, work_items, reset_failed=args.reset_failed)
        pending_rows = store.pending_items(mode, retry_failed=not args.no_retry_failed)
        print(f"Total list page pending: {len(pending_rows)}")

        if not pending_rows:
            total = export_mode_results(store, mode, Path(args.output), args.format)
            print(f"Tidak ada pending. Exported {total} rows to {args.output}")
            return 0

        backoff = FailureBackoff(args.adaptive_error_threshold, args.adaptive_cooldown_base, args.adaptive_cooldown_max)
        processed_since_restart = 0
        processed_total = 0
        current_page_obj: Page | None = None

        with sync_playwright() as playwright:
            session = BrowserSession(playwright, build_browser_settings(args, width=1440, height=1000))
            try:
                for index, row in enumerate(pending_rows, start=1):
                    item = row_to_item(row)
                    item_key = row["item_key"]
                    page_number = int(item["page_number"])

                    if processed_since_restart >= args.restart_browser_every:
                        if current_page_obj is not None:
                            current_page_obj.close()
                            current_page_obj = None
                        session.restart(f"every_{args.restart_browser_every}_pages")
                        processed_since_restart = 0

                    if args.cooldown_every > 0 and processed_total > 0 and processed_total % args.cooldown_every == 0:
                        random_cooldown(args.cooldown_min, args.cooldown_max, f"every_{args.cooldown_every}_pages")

                    print(f"[{index}/{len(pending_rows)}] Scraping list page={page_number}")
                    try:
                        if current_page_obj is None or current_page_obj.is_closed():
                            current_page_obj = open_list_page(session, url, args.entries_per_page, page_number)
                        elif index > 1:
                            # Dalam alur normal page aktif sudah berada di halaman berikutnya.
                            pass

                        records = extract_dataframe_table(current_page_obj)
                        for record in records:
                            record["_scraped_page"] = page_number
                            record["_page_label"] = get_page_label(current_page_obj)

                        store.mark_success(mode, item_key, records)
                        backoff.record_success()
                        processed_since_restart += 1
                        processed_total += 1

                        if page_number < args.end_page:
                            if not click_next_dataframe_page(current_page_obj):
                                current_page_obj.close()
                                current_page_obj = None

                        maybe_sleep(args.delay)
                    except Exception as exc:
                        if current_page_obj is not None and not current_page_obj.is_closed():
                            save_debug(current_page_obj, mode, item_key, "list_page_failure")
                            current_page_obj.close()
                            current_page_obj = None
                        store.mark_failed(mode, item_key, exc, "scrape_list_page")
                        logger.exception("LIST_PAGE_FAILED item=%s", item_key)
                        backoff.record_failure()
            finally:
                try:
                    if current_page_obj is not None and not current_page_obj.is_closed():
                        current_page_obj.close()
                except Exception:
                    pass
                session.close()

        total = export_mode_results(store, mode, Path(args.output), args.format)
        failed_total = export_failed(store, mode, Path(args.failed) if args.failed else None)
        print(f"Exported {total} rows to {args.output}")
        if args.failed:
            print(f"Exported {failed_total} failed rows to {args.failed}")
        print(f"State summary: {store.count_by_status(mode)}")
        return 0
    finally:
        store.close()


# ============================================================
# DETAIL SCRAPER
# ============================================================

def detail_url_from_kode(kode_paket: str) -> str:
    return f"{INAPROC_REALISASI_BASE_URL}?sumber=Tender&kode={kode_paket}"


def extract_key_value_table(page: Page, source_url: str, kode_paket: str = "") -> dict[str, Any]:
    result: dict[str, Any] = {
        "kode_paket": kode_paket,
        "source_url": source_url,
        "scrape_status": "success",
        "scrape_error": "",
    }

    tables = page.locator("table")
    table_count = tables.count()

    for table_index in range(table_count):
        table = tables.nth(table_index)
        rows = table.locator("tr")
        row_count = rows.count()
        if row_count == 0:
            continue

        header_row_index = None
        header_map: dict[str, int] = {}

        for r in range(row_count):
            cells = rows.nth(r).locator("th, td")
            headers = [normalize_header(cells.nth(c).inner_text()) for c in range(cells.count())]
            required_headers = {"no", "deskripsi", "detail"}
            if required_headers.issubset(set(headers)):
                header_row_index = r
                header_map = {header: idx for idx, header in enumerate(headers)}
                break

        if header_row_index is None:
            continue

        deskripsi_idx = header_map["deskripsi"]
        detail_idx = header_map["detail"]
        used_keys = set(result.keys())

        for r in range(header_row_index + 1, row_count):
            cells = rows.nth(r).locator("th, td")
            if cells.count() <= max(deskripsi_idx, detail_idx):
                continue

            desc_cell = cells.nth(deskripsi_idx)
            detail_cell = cells.nth(detail_idx)
            desc_text = clean_text(desc_cell.inner_text())
            detail_text = clean_text(detail_cell.inner_text())

            if not desc_text:
                continue

            link = detail_cell.locator("a[href]").first
            if link.count() > 0:
                href = link.get_attribute("href")
                if href:
                    detail_text = urljoin(source_url, href)

            output_key = make_unique_key(used_keys, desc_text)
            result[output_key] = detail_text

        return transform_detail_scraped_data(result)

    result["scrape_status"] = "failed"
    result["scrape_error"] = "Tabel dengan header No | Deskripsi | Detail tidak ditemukan"
    return result


def scrape_detail_url(session: BrowserSession, url: str, kode_paket: str, timeout_ms: int) -> dict[str, Any]:
    page = session.new_page()
    failed_step = "open_detail_url"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        failed_step = "wait_detail_table"
        page.wait_for_selector("table", timeout=timeout_ms)
        failed_step = "extract_detail_table"
        return extract_key_value_table(page, source_url=url, kode_paket=kode_paket)
    except Exception as exc:
        save_debug(page, "detail", kode_paket or url, failed_step)
        raise exc
    finally:
        try:
            page.close()
        except Exception:
            pass


def load_detail_work_items_from_csv(input_csv: Path, url_column: str | None, kode_column: str | None) -> list[dict[str, Any]]:
    rows = read_csv_rows(input_csv)
    if not rows:
        return []

    fieldnames = list(rows[0].keys())
    resolved_url_column = url_column or auto_find_column(
        fieldnames,
        ["url", "detail_url", "Detail_url", "link", "source_url", "Kode Paket_url"],
    )
    resolved_kode_column = kode_column or auto_find_column(
        fieldnames,
        ["Kode Paket", "kode_paket", "Kode", "kode"],
    )

    if not resolved_url_column and not resolved_kode_column:
        raise ValueError(
            "Tidak menemukan kolom URL atau kode paket. Gunakan --url-column atau --kode-column. "
            f"Kolom tersedia: {', '.join(fieldnames)}"
        )

    items: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        kode = clean_text(row.get(resolved_kode_column, "")) if resolved_kode_column else ""
        url = clean_text(row.get(resolved_url_column, "")) if resolved_url_column else ""
        if not url and kode:
            url = detail_url_from_kode(kode)
        if url:
            items.append({"source_row_id": idx, "kode_paket": kode, "url": url, "raw": row})

    return items


def load_detail_work_items_from_args(args: argparse.Namespace, db_conn=None, db_config: DbConfig | None = None) -> list[dict[str, Any]]:
    if args.input_csv:
        items = load_detail_work_items_from_csv(Path(args.input_csv), args.url_column, args.kode_column)
        return apply_start_limit(items, args.start_row, args.limit)

    if args.kode_paket:
        items = [
            {"source_row_id": idx, "kode_paket": kode, "url": detail_url_from_kode(kode), "raw": {}}
            for idx, kode in enumerate(args.kode_paket, start=1)
        ]
        return apply_start_limit(items, args.start_row, args.limit)

    if args.from_db:
        if db_conn is None or db_config is None:
            raise ValueError("Koneksi DB belum tersedia.")

        return fetch_detail_items_missing_instansi_from_db(
            conn=db_conn,
            config=db_config,
            start_row=args.start_row,
            limit=args.limit,
        )

    raise ValueError("Sumber detail belum ditentukan. Gunakan --input-csv, --kode-paket, atau --from-db.")


def load_field_map(path: str | None) -> dict[str, str]:
    if not path:
        return DEFAULT_DETAIL_DB_FIELD_MAP.copy()
    mapping = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(mapping, dict):
        raise ValueError("File mapping harus berupa JSON object.")
    return {str(k): str(v) for k, v in mapping.items()}


def scrape_detail_command(args: argparse.Namespace) -> int:
    mode = "detail"
    store = OperationalStore(Path(args.state_db))
    field_map = load_field_map(args.field_map)

    db_config = DbConfig.from_args(args)
    db_conn = None
    if args.from_db or args.update_db:
        db_conn = get_db_connection(db_config)

    try:
        if args.export_only:
            total = export_mode_results(store, mode, Path(args.output), args.format)
            failed_total = export_failed(store, mode, Path(args.failed) if args.failed else None)
            print(f"Exported {total} detail rows to {args.output}")
            if args.failed:
                print(f"Exported {failed_total} failed rows to {args.failed}")
            return 0

        work_items = load_detail_work_items_from_args(args, db_conn=db_conn, db_config=db_config)
        store.upsert_items(mode, work_items, reset_failed=args.reset_failed)
        pending_rows = store.pending_items(mode, retry_failed=not args.no_retry_failed)
        print(f"Total detail pending: {len(pending_rows)}")

        backoff = FailureBackoff(args.adaptive_error_threshold, args.adaptive_cooldown_base, args.adaptive_cooldown_max)
        processed_since_restart = 0
        processed_total = 0
        success_count = failed_count = db_success = db_failed = 0

        with sync_playwright() as playwright:
            session = BrowserSession(playwright, build_browser_settings(args))
            try:
                for index, row in enumerate(pending_rows, start=1):
                    item = row_to_item(row)
                    item_key = row["item_key"]
                    kode_paket = clean_text(item.get("kode_paket", ""))
                    url = clean_text(item.get("url", ""))

                    if processed_since_restart >= args.restart_browser_every:
                        session.restart(f"every_{args.restart_browser_every}_items")
                        processed_since_restart = 0

                    if args.cooldown_every > 0 and processed_total > 0 and processed_total % args.cooldown_every == 0:
                        random_cooldown(args.cooldown_min, args.cooldown_max, f"every_{args.cooldown_every}_items")

                    print(f"[{index}/{len(pending_rows)}] Scraping detail kode_paket={kode_paket or '-'}")
                    try:
                        scraped_data = scrape_detail_url(
                            session=session,
                            url=url,
                            kode_paket=kode_paket,
                            timeout_ms=args.timeout,
                        )

                        scraped_data["db_update_status"] = "skipped"
                        scraped_data["db_update_error"] = ""
                        scraped_data["db_affected_rows"] = 0

                        if scraped_data.get("scrape_status") == "success":
                            if args.update_db:
                                if not kode_paket:
                                    scraped_data["db_update_status"] = "failed"
                                    scraped_data["db_update_error"] = "kode_paket kosong, DB update dilewati."
                                    db_failed += 1
                                else:
                                    payload = build_update_payload(scraped_data, field_map)
                                    ok, error, affected_rows = update_detail_row(
                                        conn=db_conn,
                                        config=db_config,
                                        kode_paket=kode_paket,
                                        payload=payload,
                                    )
                                    scraped_data["db_update_status"] = "success" if ok else "failed"
                                    scraped_data["db_update_error"] = error
                                    scraped_data["db_affected_rows"] = affected_rows
                                    db_success += 1 if ok else 0
                                    db_failed += 0 if ok else 1

                            store.mark_success(mode, item_key, scraped_data)
                            success_count += 1
                            backoff.record_success()
                        else:
                            store.mark_failed(mode, item_key, scraped_data.get("scrape_error", "unknown"), "scrape_detail")
                            failed_count += 1
                            backoff.record_failure()

                        processed_since_restart += 1
                        processed_total += 1
                        maybe_sleep(args.delay)
                    except Exception as exc:
                        store.mark_failed(mode, item_key, exc, "scrape_detail")
                        failed_count += 1
                        processed_since_restart += 1
                        processed_total += 1
                        logger.exception("DETAIL_FAILED item=%s", item_key)
                        backoff.record_failure()
            finally:
                session.close()

        total = export_mode_results(store, mode, Path(args.output), args.format)
        failed_total = export_failed(store, mode, Path(args.failed) if args.failed else None)
        print(f"Exported {total} detail rows to {args.output}")
        if args.failed:
            print(f"Exported {failed_total} failed rows to {args.failed}")
        print(f"Scrape sukses: {success_count}")
        print(f"Scrape gagal: {failed_count}")
        if args.update_db:
            print(f"DB update sukses: {db_success}")
            print(f"DB update gagal: {db_failed}")
        print(f"State summary: {store.count_by_status(mode)}")
        return 0
    finally:
        if db_conn:
            db_conn.close()
        store.close()


# ============================================================
# PARTICIPANT SCRAPER
# ============================================================

def wait_for_any_selector(page: Page, selectors: list[str], timeout: int = DEFAULT_TIMEOUT_MS):
    last_error = None
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=timeout)
            return locator, selector
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Tidak ada selector yang ditemukan dari kandidat: {selectors}. Last error: {last_error}")


def open_home(page: Page, timeout: int = DEFAULT_TIMEOUT_MS):
    page.goto(SPSE_BASE_URL, wait_until="domcontentloaded", timeout=timeout)
    search_locator, matched_selector = wait_for_any_selector(page, SPSE_SEARCH_SELECTORS, timeout=timeout)
    logger.info("OPEN_HOME_MATCHED_SELECTOR selector='%s'", matched_selector)
    return search_locator


def search_instansi(page: Page, nama_instansi: str, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    search_box, matched_selector = wait_for_any_selector(page, SPSE_SEARCH_SELECTORS, timeout=timeout)
    logger.info("SEARCH_INSTANSI_INPUT_SELECTOR instansi='%s' selector='%s'", nama_instansi, matched_selector)
    search_box.fill(nama_instansi)
    first_result = page.locator(".select2-results__option, .tt-suggestion, li").first
    first_result.wait_for(state="visible", timeout=timeout)
    first_result.click()
    page.wait_for_timeout(500)


def click_masuk(page: Page, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    page.get_by_role("button", name=re.compile("Masuk", re.I)).click()
    page.wait_for_selector('a:has-text("CARI PAKET"), a:has-text("Cari Paket")', timeout=timeout)


def open_cari_paket(page: Page, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    page.get_by_role("link", name=re.compile("CARI PAKET", re.I)).click()
    page.wait_for_selector('select[name="tahun"]', timeout=timeout)


def select_tahun(page: Page, year: str, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    page.locator('select[name="tahun"]').select_option(str(year))
    page.wait_for_selector('#tbllelang_filter input[type="search"]', timeout=timeout)


def search_kode_paket(page: Page, kode_paket: str, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    search_input = page.locator('#tbllelang_filter input[type="search"]')
    search_input.wait_for(state="visible", timeout=timeout)
    search_input.fill(str(kode_paket))
    search_input.press("Enter")
    page.wait_for_selector("#tbllelang tbody tr", timeout=timeout)


def open_detail_paket(page: Page, timeout: int = DEFAULT_TIMEOUT_MS) -> Page:
    rows = page.locator("#tbllelang tbody tr")
    first_row = rows.first
    first_row.wait_for(state="visible", timeout=timeout)

    nama_paket_link = first_row.locator("a").first
    nama_paket_link.wait_for(state="visible", timeout=timeout)
    old_url = page.url

    try:
        with page.expect_popup(timeout=min(timeout, 10_000)) as popup_info:
            nama_paket_link.click()
        detail_page = popup_info.value
        detail_page.wait_for_selector("table, a:has-text('Peserta'), button:has-text('Peserta')", timeout=timeout)
        logger.info("DETAIL_PAGE_OPENED mode='popup' url='%s'", detail_page.url)
        return detail_page
    except PlaywrightTimeoutError:
        if page.url != old_url:
            page.wait_for_selector("table, a:has-text('Peserta'), button:has-text('Peserta')", timeout=timeout)
            logger.info("DETAIL_PAGE_OPENED mode='same_page' url='%s'", page.url)
            return page
        nama_paket_link.click()
        page.wait_for_selector("table, a:has-text('Peserta'), button:has-text('Peserta')", timeout=timeout)
        logger.info("DETAIL_PAGE_OPENED mode='same_page' url='%s'", page.url)
        return page


def open_tab_peserta(page: Page, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    try:
        page.get_by_role("link", name=re.compile("Peserta", re.I)).click()
    except Exception:
        page.locator("a:has-text('Peserta'), button:has-text('Peserta')").first.click()
    page.wait_for_selector("table tbody tr", timeout=timeout)


def choose_participant_table(page: Page):
    tables = page.locator("table")
    table_count = tables.count()
    best_table = tables.last

    for idx in range(table_count):
        table = tables.nth(idx)
        header_text = " ".join(table.locator("thead tr th").all_inner_texts()).lower()
        body_text = " ".join(table.locator("tbody tr").first.locator("td").all_inner_texts()).lower() if table.locator("tbody tr").count() > 0 else ""
        combined = f"{header_text} {body_text}"
        if any(keyword in combined for keyword in ["peserta", "npwp", "penawaran", "terkoreksi"]):
            best_table = table
    return best_table


def scrape_peserta_table(page: Page, kode_paket: str, timeout: int = DEFAULT_TIMEOUT_MS) -> list[dict[str, Any]]:
    table = choose_participant_table(page)
    table.wait_for(timeout=timeout)

    headers = [clean_text(header) for header in table.locator("thead tr th").all_inner_texts()]
    if not headers:
        first_row_cells = table.locator("tbody tr").first.locator("td").count()
        headers = [f"kolom_{i + 1}" for i in range(first_row_cells)]

    rows: list[dict[str, Any]] = []
    body_rows = table.locator("tbody tr")
    total_rows = body_rows.count()

    for i in range(total_rows):
        cells = [clean_text(cell) for cell in body_rows.nth(i).locator("td").all_inner_texts()]
        if not cells:
            continue
        joined_cells = " ".join(cells).lower()
        if "tidak ada data" in joined_cells or "no data" in joined_cells:
            continue

        row_data: dict[str, Any] = {}
        for idx, cell in enumerate(cells):
            column_name = headers[idx] if idx < len(headers) else f"kolom_{idx + 1}"
            row_data[clean_text(column_name)] = cell
        row_data["Kode Paket"] = kode_paket
        rows.append(row_data)

    return rows


def scrape_participant_item(
    session: BrowserSession,
    db_conn,
    db_config: DbConfig | None,
    item: dict[str, Any],
    year: str,
    timeout: int,
    insert_db: bool,
) -> dict[str, Any]:
    nama_instansi = clean_text(item.get("nama_instansi", item.get("Nama Instansi", "")))
    kode_paket = clean_text(item.get("kode_paket", item.get("Kode Paket", "")))
    page = session.new_page()
    detail_page: Page | None = None
    active_page = page
    failed_step = "open_page"

    try:
        failed_step = "open_home"
        run_step(failed_step, lambda: open_home(page, timeout=timeout))

        failed_step = "search_instansi"
        run_step(failed_step, lambda: search_instansi(page, nama_instansi, timeout=timeout))

        failed_step = "click_masuk"
        run_step(failed_step, lambda: click_masuk(page, timeout=timeout))

        failed_step = "open_cari_paket"
        run_step(failed_step, lambda: open_cari_paket(page, timeout=timeout))

        failed_step = "select_tahun"
        run_step(failed_step, lambda: select_tahun(page, year, timeout=timeout))

        failed_step = "search_kode_paket"
        run_step(failed_step, lambda: search_kode_paket(page, kode_paket, timeout=timeout))

        failed_step = "open_detail_paket"
        detail_page = run_step(failed_step, lambda: open_detail_paket(page, timeout=timeout))
        detail_page.set_default_timeout(timeout)
        active_page = detail_page

        failed_step = "open_tab_peserta"
        run_step(failed_step, lambda: open_tab_peserta(detail_page, timeout=timeout))

        failed_step = "scrape_peserta_table"
        peserta_rows = run_step(failed_step, lambda: scrape_peserta_table(detail_page, kode_paket, timeout=timeout))

        inserted_count = 0
        failed_insert_count = 0
        failed_inserts: list[dict[str, Any]] = []
        if insert_db:
            if db_conn is None or db_config is None:
                raise ValueError("insert_db aktif, tetapi koneksi DB belum tersedia.")
            participants_for_db = [normalize_participant_row_for_db(row) for row in peserta_rows]
            participants_for_db = [
                row
                for row in participants_for_db
                if row.get("nama_peserta") or row.get("npwp") or row.get("harga_penawaran") or row.get("harga_terkoreksi")
            ]
            inserted_count, failed_insert_count, failed_inserts = insert_participants_to_db(
                conn=db_conn,
                config=db_config,
                participants=participants_for_db,
            )

        result = []
        for row in peserta_rows:
            result.append(
                {
                    **row,
                    "_nama_instansi": nama_instansi,
                    "_kode_paket": kode_paket,
                    "_db_inserted_count": inserted_count,
                    "_db_failed_count": failed_insert_count,
                    "_db_failed_inserts": json.dumps(failed_inserts, ensure_ascii=False) if failed_inserts else "",
                }
            )
        return {"rows": result, "failed_step": "", "error": ""}
    except Exception as exc:
        save_debug(active_page, "participant", kode_paket, failed_step)
        raise RuntimeError(f"{failed_step}: {exc}") from exc
    finally:
        try:
            if detail_page and detail_page != page and not detail_page.is_closed():
                detail_page.close()
        except Exception:
            pass
        try:
            if page and not page.is_closed():
                page.close()
        except Exception:
            pass


def load_participant_work_items_from_csv(input_csv: Path) -> list[dict[str, Any]]:
    rows = read_csv_rows(input_csv)
    if not rows:
        return []

    fieldnames = list(rows[0].keys())
    instansi_col = auto_find_column(fieldnames, ["Nama Instansi", "nama_instansi", "Instansi"])
    kode_col = auto_find_column(fieldnames, ["Kode Paket", "kode_paket", "Kode"])

    if not instansi_col or not kode_col:
        raise ValueError(
            "CSV peserta wajib memiliki kolom Nama Instansi dan Kode Paket. "
            f"Kolom tersedia: {', '.join(fieldnames)}"
        )

    return [
        {
            "source_row_id": idx,
            "nama_instansi": clean_text(row.get(instansi_col, "")),
            "kode_paket": clean_text(row.get(kode_col, "")),
        }
        for idx, row in enumerate(rows, start=1)
    ]


def load_participant_work_items(args: argparse.Namespace, db_conn=None, db_config: DbConfig | None = None) -> list[dict[str, Any]]:
    if args.input_csv:
        items = load_participant_work_items_from_csv(Path(args.input_csv))
        return apply_start_limit(items, args.start_row, args.limit)

    if args.from_db:
        if not args.nama_instansi:
            raise ValueError("--nama-instansi wajib diisi untuk --from-db.")
        if not args.year:
            raise ValueError("--year wajib diisi untuk --from-db.")
        if db_conn is None or db_config is None:
            raise ValueError("Koneksi DB belum tersedia.")
        return load_work_items_from_db(
            conn=db_conn,
            config=db_config,
            year=str(args.year),
            nama_instansi=args.nama_instansi,
            start=(args.start_row - 1) if args.start_row else 0,
            limit=args.limit,
        )

    raise ValueError("Sumber peserta belum ditentukan. Gunakan --input-csv atau --from-db.")


def scrape_participant_command(args: argparse.Namespace) -> int:
    mode = "participant"
    store = OperationalStore(Path(args.state_db))
    db_config = DbConfig.from_args(args)
    db_conn = None
    if args.from_db or args.insert_db:
        db_conn = get_db_connection(db_config)

    try:
        if args.export_only:
            total = export_mode_results(store, mode, Path(args.output), args.format)
            failed_total = export_failed(store, mode, Path(args.failed) if args.failed else None)
            print(f"Exported {total} participant rows to {args.output}")
            if args.failed:
                print(f"Exported {failed_total} failed rows to {args.failed}")
            return 0

        work_items = load_participant_work_items(args, db_conn=db_conn, db_config=db_config)
        store.upsert_items(mode, work_items, reset_failed=args.reset_failed)
        pending_rows = store.pending_items(mode, retry_failed=not args.no_retry_failed)
        print(f"Total participant pending: {len(pending_rows)}")

        backoff = FailureBackoff(args.adaptive_error_threshold, args.adaptive_cooldown_base, args.adaptive_cooldown_max)
        processed_since_restart = 0
        processed_total = 0
        success_count = failed_count = 0

        with sync_playwright() as playwright:
            session = BrowserSession(playwright, build_browser_settings(args))
            try:
                for index, row in enumerate(pending_rows, start=1):
                    item = row_to_item(row)
                    item_key = row["item_key"]
                    nama_instansi = clean_text(item.get("nama_instansi", item.get("Nama Instansi", "")))
                    kode_paket = clean_text(item.get("kode_paket", item.get("Kode Paket", "")))

                    if not nama_instansi or not kode_paket:
                        store.mark_failed(mode, item_key, "Nama Instansi atau Kode Paket kosong.", "validate_input")
                        failed_count += 1
                        continue

                    if processed_since_restart >= args.restart_browser_every:
                        session.restart(f"every_{args.restart_browser_every}_items")
                        processed_since_restart = 0

                    if args.cooldown_every > 0 and processed_total > 0 and processed_total % args.cooldown_every == 0:
                        random_cooldown(args.cooldown_min, args.cooldown_max, f"every_{args.cooldown_every}_items")

                    print(f"[{index}/{len(pending_rows)}] Scraping peserta kode_paket={kode_paket}")
                    try:
                        result = scrape_participant_item(
                            session=session,
                            db_conn=db_conn,
                            db_config=db_config,
                            item=item,
                            year=str(args.year),
                            timeout=args.timeout,
                            insert_db=args.insert_db,
                        )
                        store.mark_success(mode, item_key, result["rows"])
                        success_count += 1
                        backoff.record_success()
                    except Exception as exc:
                        failed_step = str(exc).split(":", 1)[0]
                        store.mark_failed(mode, item_key, exc, failed_step)
                        failed_count += 1
                        logger.exception("PARTICIPANT_FAILED item=%s", item_key)
                        backoff.record_failure()

                    processed_since_restart += 1
                    processed_total += 1
                    maybe_sleep(args.delay)
            finally:
                session.close()

        total = export_mode_results(store, mode, Path(args.output), args.format)
        failed_total = export_failed(store, mode, Path(args.failed) if args.failed else None)
        print(f"Exported {total} participant rows to {args.output}")
        if args.failed:
            print(f"Exported {failed_total} failed rows to {args.failed}")
        print(f"Scrape sukses: {success_count}")
        print(f"Scrape gagal: {failed_count}")
        print(f"State summary: {store.count_by_status(mode)}")
        return 0
    finally:
        if db_conn:
            db_conn.close()
        store.close()


# ============================================================
# CLI
# ============================================================

def add_common_browser_args(parser: argparse.ArgumentParser, default_headless: bool) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--headless", dest="headless", action="store_true", help="Jalankan browser tanpa UI.")
    group.add_argument("--headful", dest="headless", action="store_false", help="Tampilkan browser saat scraping.")
    parser.set_defaults(headless=default_headless)

    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_MS, help="Timeout Playwright dalam milidetik.")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay antar item/halaman dalam detik.")
    parser.add_argument("--block-resources", default=DEFAULT_BLOCK_RESOURCES, help="Resource type yang diblok, pisahkan koma.")
    parser.add_argument("--block-url-keywords", default=DEFAULT_BLOCK_URL_KEYWORDS, help="Keyword URL yang diblok, pisahkan koma.")
    parser.add_argument("--restart-browser-every", type=int, default=750, help="Restart Chromium setiap N item/page.")
    parser.add_argument("--cooldown-every", type=int, default=1000, help="Cooldown periodik setiap N item/page. 0 untuk off.")
    parser.add_argument("--cooldown-min", type=float, default=5.0, help="Minimum cooldown periodik dalam detik.")
    parser.add_argument("--cooldown-max", type=float, default=10.0, help="Maksimum cooldown periodik dalam detik.")
    parser.add_argument("--adaptive-error-threshold", type=int, default=2, help="Mulai adaptive cooldown setelah N error beruntun.")
    parser.add_argument("--adaptive-cooldown-base", type=float, default=5.0, help="Base adaptive cooldown dalam detik.")
    parser.add_argument("--adaptive-cooldown-max", type=float, default=90.0, help="Maksimum adaptive cooldown dalam detik.")
    parser.add_argument("--state-db", default="state/inaproc_state.sqlite", help="SQLite operational storage untuk resume.")
    parser.add_argument("--export-only", action="store_true", help="Hanya export hasil sukses dari state DB ke output final.")
    parser.add_argument("--reset-failed", action="store_true", help="Set status failed menjadi pending saat mulai run.")
    parser.add_argument("--no-retry-failed", action="store_true", help="Jangan retry item failed pada run ini.")


def add_db_args(parser: argparse.ArgumentParser) -> None:
    db_group = parser.add_argument_group("Database")
    db_group.add_argument("--db-host", default=None, help="DB host. Default env INAPROC_DB_HOST atau localhost.")
    db_group.add_argument("--db-port", type=int, default=None, help="DB port. Default env INAPROC_DB_PORT atau 5432.")
    db_group.add_argument("--db-name", default=None, help="DB name. Default env INAPROC_DB_NAME atau inaproc.")
    db_group.add_argument("--db-user", default=None, help="DB user. Default env INAPROC_DB_USER.")
    db_group.add_argument("--db-password", default=None, help="DB password. Default env INAPROC_DB_PASSWORD.")
    db_group.add_argument("--db-schema", default=None, help="DB schema. Default env INAPROC_DB_SCHEMA atau tender.")
    db_group.add_argument("--db-detail-table", default=None, help="Tabel detail. Default details.")
    db_group.add_argument("--db-participant-table", default=None, help="Tabel peserta. Default participant.")
    db_group.add_argument("--db-key-field", default=None, help="Field kunci paket. Default kode_paket.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified INAProc scraper: list, detail, participant. VPS friendly dan resumable.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="Scrape daftar paket dari data.inaproc.id/realisasi.")
    list_parser.add_argument("--url", default=None, help="Custom URL override. Jika kosong, URL dibuat dari --year.")
    list_parser.add_argument("--year", type=int, default=2025, help="Tahun paket.")
    list_parser.add_argument("--start-page", type=int, default=1, help="Halaman awal.")
    list_parser.add_argument("--end-page", "--max-pages", dest="end_page", type=int, default=1, help="Halaman akhir.")
    list_parser.add_argument("--output", default="output/list_final.csv", help="Path output final CSV/JSON.")
    list_parser.add_argument("--failed", default="output/list_failed.csv", help="Path CSV failed rows.")
    list_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Format output.")
    list_parser.add_argument("--entries-per-page", type=int, default=50, help="Jumlah entri per halaman. Gunakan 0 untuk tidak mengubah.")
    add_common_browser_args(list_parser, default_headless=True)
    list_parser.set_defaults(func=scrape_list_command)

    detail_parser = subparsers.add_parser(
        "detail",
        help="Scrape detail tender. Default dari DB: ambil kode_paket yang instansi masih NULL/kosong.",
    )
    detail_source = detail_parser.add_mutually_exclusive_group(required=False)
    detail_source.add_argument("--input-csv", default=None, help="CSV sumber. Bisa berisi kolom URL atau Kode Paket.")
    detail_source.add_argument("--kode-paket", action="append", default=None, help="Kode paket. Bisa dipakai berulang.")
    detail_source.add_argument(
        "--from-db",
        action="store_true",
        help="Ambil kode_paket dari DB tender.details dengan filter instansi NULL/kosong.",
    )
    detail_parser.add_argument("--url-column", default=None, help="Nama kolom URL dalam CSV.")
    detail_parser.add_argument("--kode-column", default=None, help="Nama kolom kode paket dalam CSV.")
    # Deprecated untuk detail flow baru.
    # Dibiarkan agar command lama tidak langsung error, tetapi tidak dipakai.
    detail_parser.add_argument(
        "--nama-instansi",
        default=None,
        help=argparse.SUPPRESS,
    )
    detail_parser.add_argument(
        "--year",
        default=None,
        help=argparse.SUPPRESS,
    )
    detail_parser.add_argument("--output", default="output/detail_final.csv", help="Path output final CSV/JSON dari state.")
    detail_parser.add_argument("--failed", default="output/detail_failed.csv", help="Path CSV failed rows.")
    detail_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Format output.")
    detail_parser.add_argument(
        "--start-row",
        "--start",
        dest="start_row",
        type=int,
        default=None,
        help="Mulai dari row ke-n, basis 1. Alias: --start.",
    )
    detail_parser.add_argument("--limit", type=int, default=None, help="Batasi jumlah item.")
    detail_parser.add_argument("--update-db", action="store_true", help="Update DB tender.details menggunakan hasil scrape.")
    detail_parser.add_argument("--field-map", default=None, help="JSON mapping label hasil scrape ke kolom DB.")
    add_common_browser_args(detail_parser, default_headless=True)
    add_db_args(detail_parser)
    detail_parser.set_defaults(func=scrape_detail_command)

    participant_parser = subparsers.add_parser("participant", help="Scrape peserta paket dari SPSE.")
    participant_source = participant_parser.add_mutually_exclusive_group(required=False)
    participant_source.add_argument("--input-csv", default=None, help="CSV sumber dengan kolom Nama Instansi dan Kode Paket.")
    participant_source.add_argument("--from-db", action="store_true", help="Ambil kode paket dari DB tender.details.")
    participant_parser.add_argument("--nama-instansi", default=None, help="Nama instansi untuk source DB.")
    participant_parser.add_argument("--year", required=False, help="Tahun paket yang dipilih di SPSE.")
    participant_parser.add_argument("--output", default="output/participant_final.csv", help="Path output final CSV/JSON.")
    participant_parser.add_argument("--failed", default="output/participant_failed.csv", help="Path CSV failed rows.")
    participant_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Format output.")
    participant_parser.add_argument("--start-row", type=int, default=None, help="Mulai dari row ke-n, basis 1.")
    participant_parser.add_argument("--limit", type=int, default=None, help="Batasi jumlah row/kode paket.")
    participant_parser.add_argument("--insert-db", action="store_true", help="Insert hasil peserta ke DB tender.participant.")
    add_common_browser_args(participant_parser, default_headless=True)
    add_db_args(participant_parser)
    participant_parser.set_defaults(func=scrape_participant_command)

    args = parser.parse_args(argv)

    # Source tetap wajib kecuali export-only, agar bisa export dari state tanpa menyediakan input lagi.
    if args.command == "detail" and not args.export_only:
        # Flow baru: kalau source tidak disebutkan, default ambil dari DB
        # dengan filter instansi IS NULL.
        if not args.input_csv and not args.kode_paket and not args.from_db:
            args.from_db = True
    if args.command == "participant" and not args.export_only:
        if not args.input_csv and not args.from_db:
            parser.error("participant membutuhkan --input-csv atau --from-db kecuali --export-only.")
        if not args.year:
            parser.error("participant membutuhkan --year kecuali --export-only.")

    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        print("\nProses dihentikan oleh user.")
        return 130
    except Exception as exc:
        logger.exception("FATAL_ERROR")
        print(f"Terjadi error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())