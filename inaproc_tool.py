#!/usr/bin/env python3
"""
Unified INAProc scraping tool.

Fungsi utama:
1. list        : scrape daftar paket dari https://data.inaproc.id/realisasi
2. detail      : scrape detail paket dari URL/kode paket/DB, backup ke CSV/JSONL, opsional update DB
3. participant : scrape peserta paket dari SPSE, backup ke CSV, opsional insert DB

Contoh singkat:
  python inaproc_tool.py list --year 2025 --start-page 1 --end-page 3 --output output/paket.csv
  python inaproc_tool.py detail --input-csv output/paket.csv --kode-column "Kode Paket" --output output/detail.csv
  python inaproc_tool.py participant --input-csv output/paket.csv --year 2025 --output output/peserta.csv --failed output/peserta_failed.csv

DB config dapat diberikan lewat argument atau environment variable:
  INAPROC_DB_HOST, INAPROC_DB_PORT, INAPROC_DB_NAME, INAPROC_DB_USER, INAPROC_DB_PASSWORD
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
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
except ImportError:  # psycopg2 hanya wajib saat menggunakan fitur DB.
    psycopg2 = None
    RealDictCursor = None

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
    "Tanggal Tender": "tanggal_tender",
    "tanggal_tender_mulai": "tanggal_tender_mulai",
    "tanggal_tender_selesai": "tanggal_tender_selesai",
    "durasi_tender": "durasi_tender",
}



# ============================================================
# BROWSER RESOURCE POLICY
# ============================================================

DEFAULT_BLOCKED_RESOURCE_TYPES = ["image", "media", "font"]
DEFAULT_BLOCKED_URL_KEYWORDS = [
    "googletagmanager",
    "google-analytics",
    "analytics",
    "doubleclick",
    "adservice",
    "facebook",
    "hotjar",
    "clarity",
    "segment",
]

LOW_RESOURCE_CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-extensions",
    "--disable-sync",
    "--disable-translate",
    "--metrics-recording-only",
    "--mute-audio",
    "--no-first-run",
    "--disable-popup-blocking",
    "--disable-renderer-backgrounding",
    "--disable-backgrounding-occluded-windows",
    "--disable-ipc-flooding-protection",
]


def parse_csv_option(value: str | None) -> list[str]:
    if not value:
        return []

    items = []
    for item in str(value).split(","):
        item = item.strip().lower()
        if item and item not in {"none", "false", "0", "-"}:
            items.append(item)

    return items


def build_chromium_launch_args(args: argparse.Namespace, automation_controlled: bool = False) -> list[str]:
    launch_args = []

    if getattr(args, "low_resource_mode", True):
        launch_args.extend(LOW_RESOURCE_CHROMIUM_ARGS)
    else:
        launch_args.extend(["--no-sandbox", "--disable-dev-shm-usage"])

    if automation_controlled:
        launch_args.append("--disable-blink-features=AutomationControlled")

    # Preserve order while removing duplicates.
    return list(dict.fromkeys(launch_args))


def apply_resource_blocking(context, args: argparse.Namespace) -> None:
    if getattr(args, "no_resource_blocking", False):
        logger.info("RESOURCE_BLOCKING disabled")
        return

    blocked_types = set(parse_csv_option(getattr(args, "block_resources", "")))
    blocked_keywords = parse_csv_option(getattr(args, "block_url_keywords", ""))

    if not blocked_types and not blocked_keywords:
        logger.info("RESOURCE_BLOCKING empty_policy")
        return

    def handle_route(route, request):
        try:
            resource_type = request.resource_type
            request_url = request.url.lower()

            if resource_type in blocked_types:
                route.abort()
                return

            if any(keyword in request_url for keyword in blocked_keywords):
                route.abort()
                return

            route.continue_()
        except Exception:
            try:
                route.continue_()
            except Exception:
                pass

    context.route("**/*", handle_route)
    logger.info(
        "RESOURCE_BLOCKING enabled types=%s keywords=%s",
        sorted(blocked_types),
        blocked_keywords,
    )

# ============================================================
# LOGGING + PATHS
# ============================================================

Path("logs").mkdir(exist_ok=True)
Path("debug/screenshots").mkdir(parents=True, exist_ok=True)
Path("debug/html").mkdir(parents=True, exist_ok=True)
Path("debug/traces").mkdir(parents=True, exist_ok=True)
Path("output").mkdir(exist_ok=True)

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


def cleanse_rupiah_to_int(value: Any) -> int | None:
    """
    Contoh:
    - "Rp 367.400.000" -> 367400000
    - "Rp 367.400.000,00" -> 367400000
    """
    text = clean_text(value)

    if not text:
        return None

    text = re.sub(r"(?i)\brp\b", "", text).strip()

    # Buang desimal format Indonesia, contoh: 1.000.000,00
    if "," in text:
        text = text.split(",", 1)[0]

    digits = re.sub(r"\D+", "", text)

    if not digits:
        return None

    return int(digits)


def parse_tender_date(value: Any) -> date | None:
    """
    Contoh:
    - "28 Dec 2021" -> date(2021, 12, 28)
    - "18 Jan 2022" -> date(2022, 1, 18)
    """
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
    """
    Contoh:
    "28 Dec 2021 s/d 18 Jan 2022"
    ->
    ("2021-12-28", "2022-01-18", 21)
    """
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

    duration_days = (end_date - start_date).days

    return start_date.isoformat(), end_date.isoformat(), duration_days


def transform_detail_scraped_data(scraped_data: dict[str, Any]) -> dict[str, Any]:
    """
    Transform khusus hasil scrape detail tender sebelum disimpan ke CSV/DB.
    """
    if "Nilai HPS" in scraped_data:
        scraped_data["Nilai HPS"] = cleanse_rupiah_to_int(scraped_data.get("Nilai HPS"))

    if "Nilai Pagu" in scraped_data:
        scraped_data["Nilai Pagu"] = cleanse_rupiah_to_int(scraped_data.get("Nilai Pagu"))

    tanggal_tender_raw = scraped_data.get("Tanggal Tender")

    if tanggal_tender_raw:
        tanggal_mulai, tanggal_selesai, durasi_tender = split_tanggal_tender(tanggal_tender_raw)

        scraped_data["tanggal_tender_mulai"] = tanggal_mulai
        scraped_data["tanggal_tender_selesai"] = tanggal_selesai
        scraped_data["durasi_tender"] = durasi_tender

    return scraped_data

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


def load_env_file(path: Path = Path(".env"), override: bool = False) -> None:
    """Load .env sederhana tanpa dependency tambahan.

    Format yang didukung:
      KEY=value
      KEY="value"
      KEY='value'

    Environment variable yang sudah ada tidak ditimpa, kecuali override=True.
    """
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if not key:
            continue

        if override or key not in os.environ:
            os.environ[key] = value


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

    combined = existing_rows + records
    write_records(path, combined, output_format="csv")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row, ensure_ascii=False) + "\n")
        file.flush()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("Melewati baris JSONL invalid pada %s", path)
    return rows


def checkpoint_to_csv(checkpoint_path: Path, output_csv: Path) -> int:
    rows = read_jsonl(checkpoint_path)
    if not rows:
        print("Belum ada data checkpoint yang bisa disimpan ke CSV.")
        return 0
    write_records(output_csv, rows, output_format="csv")
    print(f"CSV backup berhasil disimpan: {output_csv}")
    return len(rows)


def apply_start_limit(items: list[dict[str, Any]], start_row: int | None, limit: int | None) -> list[dict[str, Any]]:
    if start_row is not None and start_row < 1:
        raise ValueError("--start-row harus bernilai minimal 1 jika diisi.")
    if limit is not None and limit < 1:
        raise ValueError("--limit harus bernilai minimal 1 jika diisi.")

    start_index = (start_row - 1) if start_row is not None else 0
    if start_index >= len(items):
        raise ValueError(f"--start-row {start_row} melebihi jumlah data: {len(items)}")

    return items[start_index:] if limit is None else items[start_index:start_index + limit]


def run_step(row_id: Any, step_name: str, func: Callable[[], Any]) -> Any:
    logger.info("START_STEP row=%s step=%s", row_id, step_name)
    try:
        result = func()
        logger.info("OK_STEP row=%s step=%s", row_id, step_name)
        return result
    except Exception:
        logger.exception("FAIL_STEP row=%s step=%s", row_id, step_name)
        raise


def log_page_state(page, row_id: Any, step_name: str, attempt: int | None = None) -> None:
    try:
        title = page.title()
    except Exception:
        title = "<failed_get_title>"

    try:
        url = page.url
    except Exception:
        url = "<failed_get_url>"

    logger.warning(
        "PAGE_STATE row=%s step=%s attempt=%s title='%s' url='%s'",
        row_id,
        step_name,
        attempt,
        title,
        url,
    )


def save_debug(page, row_id: Any, kode_paket: Any, step_name: str) -> None:
    debug_name = f"row_{row_id}_{safe_filename(kode_paket)}_{safe_filename(step_name)}"

    try:
        page.screenshot(path=f"debug/screenshots/{debug_name}.png", full_page=True)
    except Exception as exc:
        logger.warning("Failed screenshot row=%s step=%s: %s", row_id, step_name, exc)

    try:
        html = page.content()
        Path(f"debug/html/{debug_name}.html").write_text(html, encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed save HTML row=%s step=%s: %s", row_id, step_name, exc)


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
        raise RuntimeError(
            "psycopg2 belum ter-install. Install dengan: pip install psycopg2-binary"
        )


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
        {"source_row_id": idx, "Nama Instansi": nama_instansi, "Kode Paket": kode_paket}
        for idx, kode_paket in enumerate(kode_paket_list, start=1 + start)
    ]


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
    nama_instansi: str,
    tahun_anggaran: str,
    payload: dict[str, Any],
) -> tuple[bool, str, int]:
    if not payload:
        return False, "Tidak ada field hasil scrape yang cocok dengan mapping database.", 0

    schema = quoted_ident(config.schema)
    table = quoted_ident(config.detail_table)
    key = quoted_ident(config.key_field)

    set_clauses = []
    values = []
    for db_field, value in payload.items():
        set_clauses.append(f"{quoted_ident(db_field)} = %s")
        values.append(value)

    values.extend([kode_paket, nama_instansi, tahun_anggaran])

    query = f"""
        UPDATE {schema}.{table}
        SET {", ".join(set_clauses)}
        WHERE {key} = %s
          AND nama_instansi = %s
          AND tahun_anggaran = %s
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
        "kode_paket": clean_text(row.get("Kode Paket", "")),
    }


def insert_participants_to_db(
    conn,
    config: DbConfig,
    participants: list[dict[str, str]],
    failed_csv: Path,
    row_id: Any,
    nama_instansi: str,
    kode_paket: str,
) -> tuple[int, int]:
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
            logger.exception("DB_INSERT_FAILED row=%s kode_paket='%s' index=%s", row_id, kode_paket, idx)
            append_failed_row(
                failed_csv=failed_csv,
                row_id=row_id,
                nama_instansi=nama_instansi,
                kode_paket=kode_paket,
                failed_step="insert_participant_db",
                error_message=exc,
                extra={
                    "participant_index": idx,
                    "npwp": participant.get("npwp", ""),
                    "nama_peserta": participant.get("nama_peserta", ""),
                    "harga_penawaran": participant.get("harga_penawaran", ""),
                    "harga_terkoreksi": participant.get("harga_terkoreksi", ""),
                },
            )

    return inserted_count, failed_insert_count


# ============================================================
# LIST SCRAPER: data.inaproc.id/realisasi
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


def build_default_list_output_path(year: int, start_page: int, end_page: int, output_format: str) -> Path:
    return Path(f"output/inaproc_{year}_page_{start_page}_to_{end_page}.{output_format}")


def wait_for_dataframe_table(page, timeout: int = DEFAULT_LONG_TIMEOUT_MS) -> None:
    try:
        page.locator("table.dataframe").first.wait_for(state="attached", timeout=timeout)
    except PlaywrightTimeoutError:
        debug_path = Path("debug/screenshots/list_table_not_found.png")
        page.screenshot(path=str(debug_path), full_page=True)
        print("Table tidak ditemukan.")
        print(f"Screenshot debug disimpan ke: {debug_path}")
        print(f"Page title: {page.title()}")
        print(f"Current URL: {page.url}")
        raise


def extract_dataframe_table(page) -> list[dict[str, Any]]:
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
                    return {
                        text: cell.innerText.trim(),
                        href: link ? link.href : null
                    };
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


def get_page_label(page) -> str:
    labels = page.locator(r"text=/Halaman\s+\d+\s+dari\s+\d+/i")
    if labels.count() == 0:
        return ""
    return clean_text(labels.first.inner_text(timeout=2_000))


def get_visible_row_count(page) -> int:
    return page.locator("table.dataframe tbody tr").count()


def set_entries_per_page(page, entries_per_page: int) -> None:
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
                const caption = Array.from(document.querySelectorAll("p"))
                    .map((node) => node.innerText.trim())
                    .find((text) => /^Showing\s+\d+\s+of\s+\d+\s+records$/i.test(text)) || "";
                return selected.includes(`Selected ${targetRows}`)
                    && (rowCount >= targetRows || rowCount !== oldRowCount || caption.includes(`Showing ${targetRows}`));
            }
            """,
            arg=[entries_per_page, previous_row_count],
            timeout=60_000,
        )
    except PlaywrightTimeoutError:
        page.wait_for_timeout(2_000)


def click_next_dataframe_page(page) -> bool:
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


def go_to_start_page(page, start_page: int, delay_seconds: float) -> int:
    current_page = 1
    if start_page <= 1:
        return current_page

    print(f"Skipping dari page 1 ke page {start_page}...")
    while current_page < start_page:
        if not click_next_dataframe_page(page):
            print(f"Tidak bisa lanjut ke page {current_page + 1}. Stop di page {current_page}.")
            return current_page
        current_page += 1
        print(f"Skipped to page {current_page}")
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return current_page


def scrape_list_command(args: argparse.Namespace) -> int:
    if args.start_page < 1:
        raise ValueError("--start-page harus minimal 1.")
    if args.end_page < args.start_page:
        raise ValueError("--end-page harus lebih besar atau sama dengan --start-page.")

    url = args.url or build_realisasi_url(args.year)
    output = args.output or build_default_list_output_path(
        args.year, args.start_page, args.end_page, args.format
    )

    all_records: list[dict[str, Any]] = []
    start_time = time.perf_counter()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=args.headless,
            args=build_chromium_launch_args(args),
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            device_scale_factor=1,
        )
        apply_resource_blocking(context, args)
        page = context.new_page()
        page.set_default_timeout(args.timeout)

        try:
            print(f"Opening URL: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_LONG_TIMEOUT_MS)
            wait_for_dataframe_table(page, timeout=DEFAULT_LONG_TIMEOUT_MS)
            set_entries_per_page(page, args.entries_per_page)

            reached_page = go_to_start_page(page, args.start_page, args.delay)
            start_page = min(args.start_page, reached_page)
            if reached_page < args.start_page:
                print(f"Page awal yang diminta tidak tercapai. Dimulai dari page {reached_page}.")

            for page_number in range(start_page, args.end_page + 1):
                records = extract_dataframe_table(page)
                for record in records:
                    record["_scraped_page"] = page_number
                    record["_page_label"] = get_page_label(page)
                all_records.extend(records)
                print(f"Scraped page {page_number}: {len(records)} rows")

                if page_number >= args.end_page:
                    break
                if args.delay > 0:
                    time.sleep(args.delay)
                if not click_next_dataframe_page(page):
                    print("Tombol halaman berikutnya tidak ditemukan atau sudah disabled.")
                    break
        finally:
            try:
                context.close()
            finally:
                browser.close()

    write_records(output, all_records, output_format=args.format)
    elapsed = time.perf_counter() - start_time
    print(f"Saved {len(all_records)} rows to {output}")
    print(f"Total waktu: {elapsed:.2f} detik")
    return 0


# ============================================================
# DETAIL SCRAPER: data.inaproc.id/realisasi?sumber=Tender&kode=...
# ============================================================

def detail_url_from_kode(kode_paket: str) -> str:
    return f"{INAPROC_REALISASI_BASE_URL}?sumber=Tender&kode={kode_paket}"


def extract_key_value_table(page, source_url: str, kode_paket: str = "") -> dict[str, Any]:
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

        return result

    result["scrape_status"] = "failed"
    result["scrape_error"] = "Tabel dengan header No | Deskripsi | Detail tidak ditemukan"
    return result


def scrape_detail_url(context, url: str, kode_paket: str, timeout_ms: int) -> dict[str, Any]:
    page = context.new_page()
    page.set_default_timeout(timeout_ms)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_selector("table", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            pass
        return extract_key_value_table(page, source_url=url, kode_paket=kode_paket)
    except Exception as exc:
        return {
            "kode_paket": kode_paket,
            "source_url": url,
            "scrape_status": "failed",
            "scrape_error": str(exc),
        }
    finally:
        try:
            page.close()
        except Exception:
            pass


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
        if not args.nama_instansi:
            raise ValueError("--nama-instansi wajib diisi untuk --from-db.")
        if not args.year:
            raise ValueError("--year wajib diisi untuk --from-db.")
        if db_conn is None or db_config is None:
            raise ValueError("Koneksi DB belum tersedia.")
        work_items = load_work_items_from_db(
            conn=db_conn,
            config=db_config,
            year=str(args.year),
            nama_instansi=args.nama_instansi,
            start=0,
            limit=None,
        )
        items = [
            {
                "source_row_id": item["source_row_id"],
                "kode_paket": item["Kode Paket"],
                "url": detail_url_from_kode(item["Kode Paket"]),
                "raw": item,
            }
            for item in work_items
        ]
        return apply_start_limit(items, args.start_row, args.limit)

    raise ValueError("Sumber detail belum ditentukan. Gunakan --input-csv, --kode-paket, atau --from-db.")


def load_field_map(path: str | None) -> dict[str, str]:
    if not path:
        return DEFAULT_DETAIL_DB_FIELD_MAP.copy()
    mapping = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(mapping, dict):
        raise ValueError("File mapping harus berupa JSON object.")
    return {str(k): str(v) for k, v in mapping.items()}


def scrape_detail_command(args: argparse.Namespace) -> int:
    start_time = time.perf_counter()
    output_csv = Path(args.output)
    checkpoint = Path(args.checkpoint)
    field_map = load_field_map(args.field_map)

    db_config = DbConfig.from_args(args)
    db_conn = None
    if args.from_db or args.update_db:
        db_conn = get_db_connection(db_config)

    try:
        work_items = load_detail_work_items_from_args(args, db_conn=db_conn, db_config=db_config)
        if not work_items:
            print("Tidak ada detail work item yang bisa diproses.")
            return 0

        print(f"Total detail yang akan diproses: {len(work_items)}")
        processed = success_count = failed_count = db_success = db_failed = 0

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=args.headless,
                args=build_chromium_launch_args(args),
            )
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                device_scale_factor=1,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            apply_resource_blocking(context, args)

            try:
                for idx, item in enumerate(work_items, start=1):
                    processed += 1
                    kode_paket = clean_text(item.get("kode_paket", ""))
                    url = clean_text(item.get("url", ""))
                    print(f"[{idx}/{len(work_items)}] Scraping detail kode_paket={kode_paket or '-'}")

                    scraped_data = scrape_detail_url(
                        context=context,
                        url=url,
                        kode_paket=kode_paket,
                        timeout_ms=args.timeout,
                    )

                    scraped_data = transform_detail_scraped_data(scraped_data)

                    scraped_data["db_update_status"] = "skipped"
                    scraped_data["db_update_error"] = ""
                    scraped_data["db_affected_rows"] = 0

                    if scraped_data.get("scrape_status") == "success":
                        success_count += 1
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
                                    nama_instansi=args.nama_instansi,
                                    tahun_anggaran=str(args.year),
                                    payload=payload,
                                )
                                scraped_data["db_update_status"] = "success" if ok else "failed"
                                scraped_data["db_update_error"] = error
                                scraped_data["db_affected_rows"] = affected_rows
                                if ok:
                                    db_success += 1
                                else:
                                    db_failed += 1
                    else:
                        failed_count += 1

                    append_jsonl(checkpoint, scraped_data)

                    if args.delay > 0:
                        time.sleep(args.delay)
            finally:
                context.close()
                browser.close()

        checkpoint_to_csv(checkpoint, output_csv)
        elapsed = time.perf_counter() - start_time
        print(f"Total diproses: {processed}")
        print(f"Scrape sukses: {success_count}")
        print(f"Scrape gagal: {failed_count}")
        if args.update_db:
            print(f"DB update sukses: {db_success}")
            print(f"DB update gagal: {db_failed}")
        print(f"Total waktu: {elapsed:.2f} detik")
        return 0
    finally:
        if db_conn:
            db_conn.close()


# ============================================================
# PARTICIPANT SCRAPER: spse.inaproc.id
# ============================================================

def append_failed_row(
    failed_csv: Path,
    row_id: Any,
    nama_instansi: str,
    kode_paket: str,
    failed_step: str,
    error_message: Any,
    extra: dict[str, Any] | None = None,
) -> None:
    failed_row: dict[str, Any] = {
        "row_id": row_id,
        "Nama Instansi": nama_instansi,
        "Kode Paket": kode_paket,
        "failed_step": failed_step,
        "error_message": str(error_message),
    }
    if extra:
        failed_row.update(extra)
    append_records_csv(failed_csv, [failed_row])


def wait_for_any_selector(page, selectors: list[str], timeout: int = DEFAULT_TIMEOUT_MS):
    last_error = None
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=timeout)
            return locator, selector
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        f"Tidak ada selector yang ditemukan dari kandidat: {selectors}. Last error: {last_error}"
    )


def open_home(page, row_id: Any = None, timeout: int = DEFAULT_TIMEOUT_MS, max_attempts: int = 3):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("OPEN_HOME_ATTEMPT row=%s attempt=%s", row_id, attempt)
            page.goto(SPSE_BASE_URL, wait_until="domcontentloaded", timeout=timeout)
            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                logger.warning("NETWORKIDLE_TIMEOUT row=%s attempt=%s; lanjut wait selector", row_id, attempt)
            search_locator, matched_selector = wait_for_any_selector(page, SPSE_SEARCH_SELECTORS, timeout=timeout)
            logger.info("OPEN_HOME_MATCHED_SELECTOR row=%s attempt=%s selector='%s'", row_id, attempt, matched_selector)
            return search_locator
        except Exception as exc:
            last_error = exc
            logger.exception("OPEN_HOME_ATTEMPT_FAILED row=%s attempt=%s", row_id, attempt)
            log_page_state(page, row_id=row_id, step_name="open_home", attempt=attempt)
            try:
                save_debug(page, row_id, "open_home", f"open_home_attempt_{attempt}")
            except Exception as debug_error:
                logger.warning("OPEN_HOME_DEBUG_FAILED row=%s attempt=%s: %s", row_id, attempt, debug_error)
            if attempt < max_attempts:
                try:
                    page.wait_for_timeout(3000)
                    page.reload(wait_until="domcontentloaded", timeout=timeout)
                except Exception:
                    pass

    if last_error is not None:
        raise last_error
    raise RuntimeError("open_home gagal tanpa exception yang tertangkap.")


def search_instansi(page, nama_instansi: str, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    search_box, matched_selector = wait_for_any_selector(page, SPSE_SEARCH_SELECTORS, timeout=timeout)
    logger.info("SEARCH_INSTANSI_INPUT_SELECTOR instansi='%s' selector='%s'", nama_instansi, matched_selector)
    search_box.fill(nama_instansi)
    first_result = page.locator(".select2-results__option, .tt-suggestion, li").first
    first_result.wait_for(timeout=timeout)
    first_result.click()
    page.wait_for_timeout(500)


def click_masuk(page, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    page.get_by_role("button", name=re.compile("Masuk", re.I)).click()
    try:
        page.wait_for_load_state("networkidle", timeout=min(timeout, 10_000))
    except Exception:
        logger.warning("NETWORKIDLE_TIMEOUT step=click_masuk; lanjut proses")


def open_cari_paket(page, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    page.get_by_role("link", name=re.compile("CARI PAKET", re.I)).click()
    try:
        page.wait_for_load_state("networkidle", timeout=min(timeout, 10_000))
    except Exception:
        logger.warning("NETWORKIDLE_TIMEOUT step=open_cari_paket; lanjut wait selector")
    page.wait_for_selector('select[name="tahun"]', timeout=timeout)


def select_tahun(page, year: str) -> None:
    page.locator('select[name="tahun"]').select_option(str(year))
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except Exception:
        logger.warning("NETWORKIDLE_TIMEOUT step=select_tahun; lanjut proses")


def search_kode_paket(page, kode_paket: str, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    search_input = page.locator('#tbllelang_filter input[type="search"]')
    search_input.wait_for(timeout=timeout)
    search_input.fill(str(kode_paket))
    search_input.press("Enter")
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except Exception:
        logger.warning("NETWORKIDLE_TIMEOUT step=search_kode_paket; lanjut wait table")
    page.wait_for_selector("#tbllelang tbody tr", timeout=timeout)


def open_detail_paket(page, timeout: int = DEFAULT_TIMEOUT_MS):
    rows = page.locator("#tbllelang tbody tr")
    first_row = rows.first
    first_row.wait_for(timeout=timeout)

    nama_paket_link = first_row.locator("a").first
    nama_paket_link.wait_for(timeout=timeout)
    old_url = page.url

    try:
        with page.expect_popup(timeout=min(timeout, 10_000)) as popup_info:
            nama_paket_link.click()
        detail_page = popup_info.value
        detail_page.wait_for_load_state("domcontentloaded")
        try:
            detail_page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            logger.warning("NETWORKIDLE_TIMEOUT step=open_detail_paket popup")
        logger.info("DETAIL_PAGE_OPENED mode='popup' url='%s'", detail_page.url)
        return detail_page
    except PlaywrightTimeoutError:
        logger.warning("OPEN_DETAIL_NO_POPUP_DETECTED fallback='same_page_navigation'")
        if page.url != old_url:
            page.wait_for_load_state("domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                logger.warning("NETWORKIDLE_TIMEOUT step=open_detail_paket same_page")
            logger.info("DETAIL_PAGE_OPENED mode='same_page' url='%s'", page.url)
            return page
        nama_paket_link.click()
        page.wait_for_load_state("domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            logger.warning("NETWORKIDLE_TIMEOUT step=open_detail_paket same_page")
        logger.info("DETAIL_PAGE_OPENED mode='same_page' url='%s'", page.url)
        return page


def open_tab_peserta(page, timeout: int = DEFAULT_TIMEOUT_MS) -> None:
    try:
        page.get_by_role("link", name=re.compile("Peserta", re.I)).click()
    except Exception:
        page.locator("a:has-text('Peserta'), button:has-text('Peserta')").first.click()
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except Exception:
        logger.warning("NETWORKIDLE_TIMEOUT step=open_tab_peserta; lanjut wait table")
    page.wait_for_selector("table tbody tr", timeout=timeout)


def choose_participant_table(page):
    tables = page.locator("table")
    table_count = tables.count()
    best_table = tables.last

    for idx in range(table_count):
        table = tables.nth(idx)
        try:
            headers_text = " ".join(table.locator("thead tr th").all_inner_texts()).lower()
            if any(keyword in headers_text for keyword in ["peserta", "npwp", "penawaran", "terkoreksi"]):
                return table
        except Exception:
            continue

    return best_table


def scrape_peserta_table(page, kode_paket: str, timeout: int = DEFAULT_TIMEOUT_MS) -> list[dict[str, Any]]:
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
            "Nama Instansi": clean_text(row.get(instansi_col, "")),
            "Kode Paket": clean_text(row.get(kode_col, "")),
        }
        for idx, row in enumerate(rows, start=1)
    ]


def process_participant_row(
    context,
    db_conn,
    db_config: DbConfig | None,
    row_id: Any,
    nama_instansi: str,
    kode_paket: str,
    output_csv: Path,
    failed_csv: Path,
    year: str,
    timeout: int,
    debug_trace: bool,
    insert_db: bool,
) -> dict[str, Any]:
    page = context.new_page()
    page.set_default_timeout(timeout)
    detail_page = None
    active_page = page
    trace_path = f"debug/traces/row_{row_id}_{safe_filename(kode_paket)}.zip"

    if debug_trace:
        context.tracing.start(screenshots=True, snapshots=True, sources=True)

    failed_step = None
    try:
        failed_step = "open_home"
        run_step(row_id, failed_step, lambda: open_home(page, row_id=row_id, timeout=timeout))

        failed_step = "search_instansi"
        run_step(row_id, failed_step, lambda: search_instansi(page, nama_instansi, timeout=timeout))

        failed_step = "click_masuk"
        run_step(row_id, failed_step, lambda: click_masuk(page, timeout=timeout))

        failed_step = "open_cari_paket"
        run_step(row_id, failed_step, lambda: open_cari_paket(page, timeout=timeout))

        failed_step = "select_tahun"
        run_step(row_id, failed_step, lambda: select_tahun(page, year))

        failed_step = "search_kode_paket"
        run_step(row_id, failed_step, lambda: search_kode_paket(page, kode_paket, timeout=timeout))

        failed_step = "open_detail_paket"
        detail_page = run_step(row_id, failed_step, lambda: open_detail_paket(page, timeout=timeout))
        detail_page.set_default_timeout(timeout)
        active_page = detail_page

        failed_step = "open_tab_peserta"
        run_step(row_id, failed_step, lambda: open_tab_peserta(detail_page, timeout=timeout))

        failed_step = "scrape_peserta_table"
        peserta_rows = run_step(row_id, failed_step, lambda: scrape_peserta_table(detail_page, kode_paket, timeout=timeout))

        if peserta_rows:
            append_records_csv(output_csv, peserta_rows)
        else:
            logger.warning("EMPTY_RESULT row=%s instansi='%s' kode_paket='%s'", row_id, nama_instansi, kode_paket)

        inserted_count = 0
        failed_insert_count = 0
        if insert_db:
            if db_conn is None or db_config is None:
                raise ValueError("insert_db aktif, tetapi koneksi DB belum tersedia.")
            participants_for_db = [normalize_participant_row_for_db(row) for row in peserta_rows]
            participants_for_db = [
                row
                for row in participants_for_db
                if row.get("nama_peserta") or row.get("npwp") or row.get("harga_penawaran") or row.get("harga_terkoreksi")
            ]
            failed_step = "insert_participant_db"
            inserted_count, failed_insert_count = insert_participants_to_db(
                conn=db_conn,
                config=db_config,
                participants=participants_for_db,
                failed_csv=failed_csv,
                row_id=row_id,
                nama_instansi=nama_instansi,
                kode_paket=kode_paket,
            )

        logger.info(
            "SUCCESS row=%s instansi='%s' kode_paket='%s' peserta_rows=%s db_inserted=%s db_failed=%s",
            row_id,
            nama_instansi,
            kode_paket,
            len(peserta_rows),
            inserted_count,
            failed_insert_count,
        )
        return {
            "success": True,
            "failed_step": None,
            "peserta_rows": len(peserta_rows),
            "db_inserted": inserted_count,
            "db_failed": failed_insert_count,
        }

    except Exception as exc:
        save_debug(active_page, row_id, kode_paket, failed_step or "unknown")
        append_failed_row(
            failed_csv=failed_csv,
            row_id=row_id,
            nama_instansi=nama_instansi,
            kode_paket=kode_paket,
            failed_step=failed_step or "unknown",
            error_message=exc,
        )
        logger.exception(
            "FAILED row=%s instansi='%s' kode_paket='%s' step='%s'",
            row_id,
            nama_instansi,
            kode_paket,
            failed_step,
        )
        return {
            "success": False,
            "failed_step": failed_step,
            "peserta_rows": 0,
            "db_inserted": 0,
            "db_failed": 0,
        }
    finally:
        if debug_trace:
            try:
                context.tracing.stop(path=trace_path)
            except Exception as exc:
                logger.warning("Failed stop trace row=%s: %s", row_id, exc)

        try:
            if detail_page and detail_page != page and not detail_page.is_closed():
                detail_page.close()
        except Exception as exc:
            logger.warning("Failed close detail_page row=%s: %s", row_id, exc)

        try:
            if page and not page.is_closed():
                page.close()
        except Exception as exc:
            logger.warning("Failed close page row=%s: %s", row_id, exc)


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
    start_time = time.perf_counter()
    output_csv = Path(args.output)
    failed_csv = Path(args.failed)

    db_config = DbConfig.from_args(args)
    db_conn = None
    if args.from_db or args.insert_db:
        db_conn = get_db_connection(db_config)

    try:
        work_items = load_participant_work_items(args, db_conn=db_conn, db_config=db_config)
        if not work_items:
            print("Tidak ada data kerja peserta ditemukan.")
            return 0

        logger.info(
            "START_PARTICIPANT_RUN source='%s' year='%s' output_csv='%s' failed_csv='%s' headless=%s timeout=%s total=%s",
            "csv" if args.input_csv else "db",
            args.year,
            output_csv,
            failed_csv,
            args.headless,
            args.timeout,
            len(work_items),
        )

        processed_count = success_count = failed_count = skipped_count = 0
        total_peserta_rows = total_db_inserted = total_db_failed = 0
        consecutive_open_home_failures = 0

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=args.headless,
                args=build_chromium_launch_args(args, automation_controlled=True),
            )
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                device_scale_factor=1,
                accept_downloads=True,
                locale="id-ID",
                timezone_id="Asia/Jakarta",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            apply_resource_blocking(context, args)
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)

            try:
                for idx, item in enumerate(work_items, start=1):
                    row_id = item.get("source_row_id", idx)
                    nama_instansi = clean_text(item.get("Nama Instansi", ""))
                    kode_paket = clean_text(item.get("Kode Paket", ""))
                    processed_count += 1

                    if not nama_instansi or not kode_paket:
                        skipped_count += 1
                        failed_count += 1
                        append_failed_row(
                            failed_csv=failed_csv,
                            row_id=row_id,
                            nama_instansi=nama_instansi,
                            kode_paket=kode_paket,
                            failed_step="validate_input",
                            error_message="Nama Instansi atau Kode Paket kosong.",
                        )
                        continue

                    print(f"[{idx}/{len(work_items)}] Scraping peserta kode_paket={kode_paket}")
                    result = process_participant_row(
                        context=context,
                        db_conn=db_conn,
                        db_config=db_config,
                        row_id=row_id,
                        nama_instansi=nama_instansi,
                        kode_paket=kode_paket,
                        output_csv=output_csv,
                        failed_csv=failed_csv,
                        year=str(args.year),
                        timeout=args.timeout,
                        debug_trace=args.debug_trace,
                        insert_db=args.insert_db,
                    )

                    if result and result.get("success"):
                        success_count += 1
                        total_peserta_rows += int(result.get("peserta_rows", 0) or 0)
                        total_db_inserted += int(result.get("db_inserted", 0) or 0)
                        total_db_failed += int(result.get("db_failed", 0) or 0)
                    else:
                        failed_count += 1

                    if result and result.get("failed_step") == "open_home":
                        consecutive_open_home_failures += 1
                    else:
                        consecutive_open_home_failures = 0

                    if consecutive_open_home_failures >= args.max_consecutive_open_home_failures:
                        logger.error(
                            "STOP_RUN reason='open_home_failed_consecutively' count=%s",
                            consecutive_open_home_failures,
                        )
                        print(
                            f"Proses dihentikan karena open_home gagal {consecutive_open_home_failures} kali beruntun."
                        )
                        break

                    if args.delay > 0:
                        time.sleep(args.delay)
            finally:
                context.close()
                browser.close()

        elapsed = time.perf_counter() - start_time
        avg_per_row = elapsed / processed_count if processed_count else 0

        print(f"Selesai. Output tersimpan di: {output_csv}")
        print(f"Failed rows tersimpan di: {failed_csv}")
        print(f"Total row diproses: {processed_count}")
        print(f"Total row sukses scraping: {success_count}")
        print(f"Total row gagal/skipped: {failed_count}")
        print(f"Total row skipped validasi: {skipped_count}")
        print(f"Total baris peserta discrape: {total_peserta_rows}")
        if args.insert_db:
            print(f"Total insert DB sukses: {total_db_inserted}")
            print(f"Total insert DB gagal: {total_db_failed}")
        print(f"Total waktu: {elapsed:.2f} detik")
        print(f"Rata-rata per row: {avg_per_row:.2f} detik")
        return 0
    finally:
        if db_conn:
            db_conn.close()


# ============================================================
# CLI
# ============================================================

def add_common_browser_args(parser: argparse.ArgumentParser, default_headless: bool) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--headless", dest="headless", action="store_true", help="Jalankan browser tanpa UI.")
    group.add_argument("--headful", dest="headless", action="store_false", help="Tampilkan browser saat scraping.")
    parser.set_defaults(headless=default_headless, low_resource_mode=True)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_MS, help="Timeout Playwright dalam milidetik.")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay antar item/halaman dalam detik.")
    parser.add_argument(
        "--block-resources",
        default=os.getenv("INAPROC_BLOCK_RESOURCES", ",".join(DEFAULT_BLOCKED_RESOURCE_TYPES)),
        help=(
            "Resource browser yang diblokir, pisahkan dengan koma. "
            "Default aman untuk VPS: image,media,font. "
            "Contoh agresif: image,media,font,stylesheet. Gunakan 'none' untuk kosong."
        ),
    )
    parser.add_argument(
        "--block-url-keywords",
        default=os.getenv("INAPROC_BLOCK_URL_KEYWORDS", ",".join(DEFAULT_BLOCKED_URL_KEYWORDS)),
        help="Keyword URL request yang diblokir, pisahkan dengan koma. Gunakan 'none' untuk kosong.",
    )
    parser.add_argument(
        "--no-resource-blocking",
        action="store_true",
        help="Matikan semua route blocking resource.",
    )
    parser.add_argument(
        "--no-low-resource-mode",
        dest="low_resource_mode",
        action="store_false",
        help="Matikan Chromium args low-resource tambahan.",
    )


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
        description="Unified INAProc scraper: list, detail, dan participant.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="Scrape daftar paket dari data.inaproc.id/realisasi.")
    list_parser.add_argument("--url", default=None, help="Custom URL override. Jika kosong, URL dibuat dari --year.")
    list_parser.add_argument("--year", type=int, default=2025, help="Tahun paket.")
    list_parser.add_argument("--start-page", type=int, default=1, help="Halaman awal.")
    list_parser.add_argument("--end-page", "--max-pages", dest="end_page", type=int, default=1, help="Halaman akhir.")
    list_parser.add_argument("--output", type=Path, default=None, help="Path output CSV/JSON.")
    list_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Format output.")
    list_parser.add_argument("--entries-per-page", type=int, default=50, help="Jumlah entri per halaman. Gunakan 0 untuk tidak mengubah.")
    add_common_browser_args(list_parser, default_headless=True)
    list_parser.set_defaults(func=scrape_list_command)

    detail_parser = subparsers.add_parser("detail", help="Scrape detail paket dari URL/kode paket/DB.")
    detail_source = detail_parser.add_mutually_exclusive_group(required=True)
    detail_source.add_argument("--input-csv", default=None, help="CSV sumber. Bisa berisi kolom URL atau Kode Paket.")
    detail_source.add_argument("--kode-paket", action="append", default=None, help="Kode paket. Bisa dipakai berulang.")
    detail_source.add_argument("--from-db", action="store_true", help="Ambil kode paket dari DB tender.details.")
    detail_parser.add_argument("--url-column", default=None, help="Nama kolom URL dalam CSV.")
    detail_parser.add_argument("--kode-column", default=None, help="Nama kolom kode paket dalam CSV.")
    detail_parser.add_argument("--nama-instansi", default=None, help="Filter nama instansi untuk DB update/source DB.")
    detail_parser.add_argument("--year", default=None, help="Tahun anggaran untuk DB update/source DB.")
    detail_parser.add_argument("--output", default="output/detail_result.csv", help="Path output CSV final dari checkpoint.")
    detail_parser.add_argument("--checkpoint", default="output/detail_checkpoint.jsonl", help="Path checkpoint JSONL.")
    detail_parser.add_argument("--start-row", type=int, default=None, help="Mulai dari row ke-n, basis 1.")
    detail_parser.add_argument("--limit", type=int, default=None, help="Batasi jumlah item.")
    detail_parser.add_argument("--update-db", action="store_true", help="Update DB tender.details menggunakan hasil scrape.")
    detail_parser.add_argument("--field-map", default=None, help="JSON mapping label hasil scrape ke kolom DB.")
    add_common_browser_args(detail_parser, default_headless=True)
    add_db_args(detail_parser)
    detail_parser.set_defaults(func=scrape_detail_command)

    participant_parser = subparsers.add_parser("participant", help="Scrape peserta paket dari SPSE.")
    participant_source = participant_parser.add_mutually_exclusive_group(required=True)
    participant_source.add_argument("--input-csv", default=None, help="CSV sumber dengan kolom Nama Instansi dan Kode Paket.")
    participant_source.add_argument("--from-db", action="store_true", help="Ambil kode paket dari DB tender.details.")
    participant_parser.add_argument("--nama-instansi", default=None, help="Nama instansi untuk source DB.")
    participant_parser.add_argument("--year", required=True, help="Tahun paket yang dipilih di SPSE.")
    participant_parser.add_argument("--output", default="output/participant_result.csv", help="Path CSV hasil peserta.")
    participant_parser.add_argument("--failed", default="output/participant_failed.csv", help="Path CSV baris gagal.")
    participant_parser.add_argument("--start-row", type=int, default=None, help="Mulai dari row ke-n, basis 1.")
    participant_parser.add_argument("--limit", type=int, default=None, help="Batasi jumlah row/kode paket.")
    participant_parser.add_argument("--debug-trace", action="store_true", help="Aktifkan Playwright trace per row.")
    participant_parser.add_argument("--insert-db", action="store_true", help="Insert hasil peserta ke DB tender.participant.")
    participant_parser.add_argument("--max-consecutive-open-home-failures", type=int, default=3, help="Stop jika open_home gagal beruntun.")
    add_common_browser_args(participant_parser, default_headless=True)
    add_db_args(participant_parser)
    participant_parser.set_defaults(func=scrape_participant_command)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_env_file()
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