import argparse
import csv
import html
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

DEFAULT_INPUT_CSV = "sample/scraping_inaproc.csv"
DEFAULT_DOWNLOAD_DIR = "downloads/inaproc_csv"
DEFAULT_STATE_FILE = "state/inaproc_csv_download_state.json"
DEFAULT_LOG_FILE = "logs/inaproc_csv_download_log.csv"

NO_DATA_TEXT = "Tidak ada data untuk filter dan pencarian saat ini."
NO_DOWNLOAD_TEXT = "Tombol Download CSV tidak aktif; kemungkinan tidak ada data yang bisa diunduh."
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_FAILED_NO_DATA = "failed_no_data"
DOWNLOAD_BUTTON_PATTERN = re.compile("Download CSV", re.IGNORECASE)
EXPORTING_BUTTON_PATTERN = re.compile("Mengekspor", re.IGNORECASE)


# ============================================================
# DATA MODELS
# ============================================================

@dataclass(frozen=True)
class InputRow:
    row_number: int
    full_url: str
    source_filename: str


@dataclass(frozen=True)
class RowResult:
    row: InputRow
    status: str
    output_path: str = ""
    actual_filename: str = ""
    error: str = ""


@dataclass
class RunStats:
    success: int = 0
    failed: int = 0
    no_data: int = 0
    skipped: int = 0

    def record_result(self, result: RowResult) -> None:
        if result.status == STATUS_SUCCESS:
            self.success += 1
        elif result.status == STATUS_FAILED_NO_DATA:
            self.no_data += 1
        else:
            self.failed += 1


# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================

class NoDataForFilterError(Exception):
    pass


# ============================================================
# BASIC HELPERS
# ============================================================

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def sanitize_filename(value: str) -> str:
    value = str(value or "").strip()

    if not value:
        value = "inaproc_download"

    value = re.sub(r"[\\/:*?\"<>|]+", "_", value)
    value = re.sub(r"\s+", "_", value)
    value = value.strip("._ ")

    if not value.lower().endswith(".csv"):
        value += ".csv"

    return value


def filename_from_content_disposition(value: str) -> str | None:
    if not value:
        return None

    match = re.search(r"filename\*=UTF-8''([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return unquote(match.group(1).strip().strip('"'))

    match = re.search(r'filename="?([^";]+)"?', value, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return None


def make_unique_path(directory: Path, filename: str) -> Path:
    safe_name = sanitize_filename(filename)
    path = directory / safe_name

    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix

    counter = 1

    while True:
        candidate = directory / f"{stem}_{counter}{suffix}"

        if not candidate.exists():
            return candidate

        counter += 1


def parse_localized_int(value: str) -> int | None:
    digits = re.sub(r"\D+", "", value or "")

    if not digits:
        return None

    return int(digits)


def extract_visible_package_count(value: str) -> int | None:
    if not value:
        return None

    match = re.search(
        r"Jumlah\s+Paket\D{0,80}(\d[\d.,\s]*)",
        value,
        flags=re.IGNORECASE,
    )

    if not match:
        return None

    return parse_localized_int(match.group(1))


def extract_payload_package_count(value: str) -> int | None:
    if not value:
        return None

    decoded_value = html.unescape(value)
    patterns = [
        r'\\?"jumlah_paket\\?"\s*:\s*(\d+)',
        r'\\?"jumlahPaket\\?"\s*:\s*(\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, decoded_value, flags=re.IGNORECASE)

        if match:
            return int(match.group(1))

    return None


# ============================================================
# INPUT CSV
# ============================================================

def load_input_rows(input_csv: Path) -> list[InputRow]:
    if not input_csv.exists():
        raise FileNotFoundError(f"File input tidak ditemukan: {input_csv}")

    with input_csv.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if not rows:
        raise ValueError(f"File CSV kosong: {input_csv}")

    available_columns = set(rows[0].keys())

    if "full_url" not in available_columns:
        raise ValueError(
            f"Kolom wajib 'full_url' tidak ditemukan. "
            f"Kolom tersedia: {sorted(available_columns)}"
        )

    cleaned_rows: list[InputRow] = []

    for index, row in enumerate(rows, start=1):
        full_url = str(row.get("full_url") or "").strip()
        source_filename = str(row.get("filename") or "").strip()

        if not full_url:
            continue

        cleaned_rows.append(
            InputRow(
                row_number=index,
                full_url=full_url,
                source_filename=source_filename,
            )
        )

    return cleaned_rows


def apply_row_window(
    rows: list[InputRow],
    start_row: int | None,
    limit: int | None,
) -> list[InputRow]:
    if start_row is not None:
        if start_row < 1:
            raise ValueError("--start-row harus lebih besar dari 0.")

        rows = [row for row in rows if row.row_number >= start_row]

    if limit is not None:
        if limit < 1:
            raise ValueError("--limit harus lebih besar dari 0.")

        rows = rows[:limit]

    return rows


# ============================================================
# STATE / RESUME
# ============================================================

def load_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {}

    try:
        with state_file.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, dict):
            return data

        return {}

    except Exception:
        return {}


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    ensure_parent(state_file)

    temp_file = state_file.with_suffix(state_file.suffix + ".tmp")

    with temp_file.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)

    temp_file.replace(state_file)


def is_completed_in_state(state: dict[str, Any], key: str) -> bool:
    item = state.get(key)

    if not isinstance(item, dict):
        return False

    return item.get("status") in {
        STATUS_SUCCESS,
        STATUS_FAILED_NO_DATA,
    }


def mark_state(
    state: dict[str, Any],
    key: str,
    row_number: str,
    full_url: str,
    source_filename: str,
    status: str,
    output_path: str = "",
    actual_filename: str = "",
    error: str = "",
) -> None:
    state[key] = {
        "updated_at": now_iso(),
        "row_number": row_number,
        "full_url": full_url,
        "source_filename": source_filename,
        "actual_filename": actual_filename,
        "status": status,
        "output_path": output_path,
        "error": error,
    }


def persist_result(
    state_file: Path,
    log_file: Path,
    state: dict[str, Any],
    result: RowResult,
) -> None:
    row = result.row
    row_number = str(row.row_number)

    mark_state(
        state=state,
        key=row.full_url,
        row_number=row_number,
        full_url=row.full_url,
        source_filename=row.source_filename,
        actual_filename=result.actual_filename,
        status=result.status,
        output_path=result.output_path,
        error=result.error,
    )
    save_state(state_file, state)

    append_log(
        log_file=log_file,
        row_number=row_number,
        full_url=row.full_url,
        source_filename=row.source_filename,
        actual_filename=result.actual_filename,
        status=result.status,
        output_path=result.output_path,
        error=result.error,
    )


# ============================================================
# LOGGING
# ============================================================

def append_log(
    log_file: Path,
    row_number: str,
    full_url: str,
    source_filename: str,
    status: str,
    output_path: str = "",
    actual_filename: str = "",
    error: str = "",
) -> None:
    ensure_parent(log_file)

    file_exists = log_file.exists()

    with log_file.open("a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "timestamp",
                "row_number",
                "full_url",
                "source_filename",
                "actual_filename",
                "status",
                "output_path",
                "error",
            ],
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(
            {
                "timestamp": now_iso(),
                "row_number": row_number,
                "full_url": full_url,
                "source_filename": source_filename,
                "actual_filename": actual_filename,
                "status": status,
                "output_path": output_path,
                "error": error,
            }
        )


# ============================================================
# DOWNLOAD DIRECTORY WATCHER
# ============================================================

def snapshot_download_dir(download_dir: Path) -> set[str]:
    download_dir.mkdir(parents=True, exist_ok=True)

    return {
        str(path.resolve())
        for path in download_dir.iterdir()
        if path.is_file()
    }


def is_temp_download_file(path: Path) -> bool:
    name = path.name.lower()
    suffix = path.suffix.lower()

    temp_indicators = [
        ".crdownload",
        ".tmp",
        ".download",
        ".part",
    ]

    return suffix in temp_indicators or any(name.endswith(item) for item in temp_indicators)


def find_new_stable_file(
    download_dir: Path,
    before_snapshot: set[str],
    last_sizes: dict[str, int],
    stable_since: dict[str, float],
    stable_seconds: float = 2.0,
) -> Path | None:
    now = time.monotonic()

    for path in download_dir.iterdir():
        if not path.is_file():
            continue

        if is_temp_download_file(path):
            continue

        key = str(path.resolve())

        if key in before_snapshot:
            continue

        try:
            size = path.stat().st_size
        except FileNotFoundError:
            continue

        if size <= 0:
            continue

        previous_size = last_sizes.get(key)

        if previous_size == size:
            if key not in stable_since:
                stable_since[key] = now

            if now - stable_since[key] >= stable_seconds:
                return path

        else:
            last_sizes[key] = size
            stable_since[key] = now

    return None


def configure_chromium_download_dir(context, page, download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)

    try:
        cdp = context.new_cdp_session(page)

        try:
            cdp.send(
                "Browser.setDownloadBehavior",
                {
                    "behavior": "allow",
                    "downloadPath": str(download_dir.resolve()),
                },
            )
        except Exception:
            cdp.send(
                "Page.setDownloadBehavior",
                {
                    "behavior": "allow",
                    "downloadPath": str(download_dir.resolve()),
                },
            )

        print(f"Download behavior diarahkan ke: {download_dir.resolve()}")

    except Exception as exc:
        print(f"Warning: gagal set Chromium download behavior: {exc}")


# ============================================================
# PAGE HELPERS
# ============================================================

def get_visible_package_count(page) -> int | None:
    try:
        body_text = page.locator("body").inner_text(timeout=2_000)
    except Exception:
        return None

    return extract_visible_package_count(body_text)


def get_payload_package_count(page) -> int | None:
    try:
        page_html = page.content()
    except Exception:
        return None

    return extract_payload_package_count(page_html)


def is_no_data_visible(page) -> bool:
    try:
        return page.get_by_text(NO_DATA_TEXT, exact=True).first.is_visible()
    except Exception:
        return False


def get_download_button_locator(page):
    return page.get_by_role("button", name=DOWNLOAD_BUTTON_PATTERN)


def is_download_button_enabled(page) -> bool:
    try:
        button = get_download_button_locator(page).first

        if not button.is_visible(timeout=1_000):
            return False

        return button.is_enabled(timeout=1_000)

    except Exception:
        return False


def get_download_button_state(page) -> str:
    try:
        button = get_download_button_locator(page).first

        if not button.is_visible(timeout=1_000):
            return "hidden"

        if button.is_enabled(timeout=1_000):
            return "enabled"

        return "disabled"

    except Exception:
        return "missing"


def wait_result_ready(page, timeout_ms: int) -> None:
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

    try:
        page.get_by_text("Data Realisasi", exact=False).first.wait_for(
            state="visible",
            timeout=min(timeout_ms, 30_000),
        )
    except Exception:
        print("    Warning: judul Data Realisasi belum terdeteksi, lanjut cek data.")

    print("    Menunggu jumlah paket, tabel data, atau pesan no-data...")

    deadline = time.monotonic() + (timeout_ms / 1000)
    last_reported_count: int | None = None
    payload_count_checked = False
    table_seen = False
    disabled_since: float | None = None
    disabled_no_data_seconds = 30.0

    while time.monotonic() < deadline:
        visible_package_count = get_visible_package_count(page)
        download_button_state = get_download_button_state(page)

        if visible_package_count is not None:
            if visible_package_count != last_reported_count:
                print(f"    Jumlah Paket terdeteksi: {visible_package_count}")
                last_reported_count = visible_package_count

            if visible_package_count <= 0:
                raise NoDataForFilterError(NO_DATA_TEXT)

            if download_button_state == "enabled":
                return

            if disabled_since is None and download_button_state == "disabled":
                disabled_since = time.monotonic()

            if (
                disabled_since is not None
                and time.monotonic() - disabled_since >= 60.0
            ):
                raise RuntimeError(
                    "Jumlah paket lebih dari 0, tetapi tombol Download CSV tetap disabled."
                )

            page.wait_for_timeout(500)
            continue

        if is_no_data_visible(page):
            raise NoDataForFilterError(NO_DATA_TEXT)

        if download_button_state == "enabled":
            print("    Tombol Download CSV aktif.")
            return

        if download_button_state == "disabled":
            if disabled_since is None:
                disabled_since = time.monotonic()
        else:
            disabled_since = None

        try:
            table_rows = page.locator("table tbody tr").count()

            if table_rows > 0:
                if not table_seen:
                    print(f"    Tabel muncul. Rows terdeteksi: {table_rows}")
                    table_seen = True

        except Exception:
            pass

        if not payload_count_checked:
            payload_count = get_payload_package_count(page)
            payload_count_checked = payload_count is not None

            if payload_count is not None:
                print(f"    Jumlah Paket dari payload terdeteksi: {payload_count}")

                if payload_count <= 0:
                    raise NoDataForFilterError(NO_DATA_TEXT)

        if (
            table_seen
            and disabled_since is not None
            and time.monotonic() - disabled_since >= disabled_no_data_seconds
        ):
            raise NoDataForFilterError(NO_DOWNLOAD_TEXT)

        page.wait_for_timeout(500)

    raise RuntimeError(
        "Jumlah paket, tabel data, atau pesan no-data tidak muncul sampai timeout. "
        "Kemungkinan URL lambat, filter kosong, atau halaman gagal load."
    )


def get_download_button(page, timeout_ms: int):
    button = get_download_button_locator(page).first
    button.wait_for(state="visible", timeout=timeout_ms)

    if not button.is_enabled(timeout=2_000):
        raise NoDataForFilterError(NO_DOWNLOAD_TEXT)

    return button


def wait_export_cycle(page, timeout_ms: int) -> None:
    print("    Menunggu tombol berubah menjadi 'Mengekspor...'...")

    exporting_seen = False

    try:
        page.get_by_role("button", name=EXPORTING_BUTTON_PATTERN).wait_for(
            state="visible",
            timeout=20_000,
        )

        exporting_seen = True
        print("    Status ekspor terdeteksi: Mengekspor...")

    except Exception:
        print("    Warning: status 'Mengekspor...' tidak sempat tertangkap.")

    print("    Menunggu tombol kembali ke 'Download CSV'...")

    try:
        get_download_button_locator(page).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )

        print("    Tombol kembali ke Download CSV.")

    except Exception as exc:
        if exporting_seen:
            raise RuntimeError(
                "Ekspor mulai berjalan, tetapi tombol tidak kembali ke Download CSV sampai timeout."
            ) from exc

        print("    Warning: siklus ekspor tidak bisa dipastikan, lanjut cek download.")


# ============================================================
# NETWORK RESPONSE HELPERS
# ============================================================

def is_csv_like_response(response) -> bool:
    try:
        headers = {k.lower(): v.lower() for k, v in response.headers.items()}
    except Exception:
        return False

    content_type = headers.get("content-type", "")
    content_disposition = headers.get("content-disposition", "")
    response_url = response.url.lower()

    indicators = [
        "text/csv" in content_type,
        "application/csv" in content_type,
        "application/vnd.ms-excel" in content_type,
        "application/octet-stream" in content_type and "csv" in content_disposition,
        "attachment" in content_disposition and "csv" in content_disposition,
        "download" in response_url and "csv" in response_url,
        "csv" in response_url and "realisasi" in response_url,
    ]

    return any(indicators)


def is_potential_export_response(response) -> bool:
    try:
        url = response.url.lower()
        headers = {k.lower(): v.lower() for k, v in response.headers.items()}
    except Exception:
        return False

    content_type = headers.get("content-type", "")
    content_disposition = headers.get("content-disposition", "")

    keywords = [
        "csv",
        "export",
        "download",
        "realisasi",
    ]

    return (
        any(keyword in url for keyword in keywords)
        or "csv" in content_type
        or "attachment" in content_disposition
        or "csv" in content_disposition
    )


# ============================================================
# DOWNLOAD LOGIC
# ============================================================

def download_csv_from_url(
    page,
    full_url: str,
    download_dir: Path,
    timeout_ms: int,
) -> Path:
    print(f"    Buka URL: {full_url}")

    page.goto(full_url, wait_until="domcontentloaded", timeout=timeout_ms)

    wait_result_ready(page, timeout_ms=timeout_ms)

    button = get_download_button(page, timeout_ms=timeout_ms)

    download_dir.mkdir(parents=True, exist_ok=True)

    before_snapshot = snapshot_download_dir(download_dir)

    download_result = {
        "done": False,
        "path": "",
        "error": "",
    }

    def handle_download(download):
        try:
            print("    Native Playwright download event terdeteksi.")

            suggested_name = download.suggested_filename or "inaproc_download.csv"
            output_path = make_unique_path(download_dir, suggested_name)

            download.save_as(output_path)

            if output_path.exists() and output_path.stat().st_size > 0:
                download_result["done"] = True
                download_result["path"] = str(output_path)
            else:
                download_result["error"] = f"Native download kosong/tidak ditemukan: {output_path}"

        except Exception as exc:
            download_result["error"] = f"Native download gagal: {exc}"

    def handle_response(response):
        if download_result["done"]:
            return

        try:
            if is_potential_export_response(response):
                headers = {k.lower(): v for k, v in response.headers.items()}

                print(
                    "    Potential export response:",
                    response.status,
                    response.url,
                    "| content-type:",
                    headers.get("content-type", ""),
                    "| content-disposition:",
                    headers.get("content-disposition", ""),
                )

            if not is_csv_like_response(response):
                return

            print(f"    CSV-like response terdeteksi: {response.url}")

            body = response.body()

            if not body:
                return

            headers = {k.lower(): v for k, v in response.headers.items()}
            content_disposition = headers.get("content-disposition", "")

            filename = (
                filename_from_content_disposition(content_disposition)
                or "inaproc_download.csv"
            )

            output_path = make_unique_path(download_dir, filename)
            output_path.write_bytes(body)

            if output_path.exists() and output_path.stat().st_size > 0:
                download_result["done"] = True
                download_result["path"] = str(output_path)

        except Exception as exc:
            download_result["error"] = f"Response capture gagal: {exc}"

    page.on("download", handle_download)
    page.on("response", handle_response)

    last_sizes: dict[str, int] = {}
    stable_since: dict[str, float] = {}

    try:
        print("    Klik Download CSV...")

        button.scroll_into_view_if_needed(timeout=10_000)

        if not button.is_enabled(timeout=2_000):
            raise NoDataForFilterError(NO_DOWNLOAD_TEXT)

        button.click(timeout=30_000)

        wait_export_cycle(page, timeout_ms=timeout_ms)

        print("    Menunggu file download/response CSV...")

        deadline = time.monotonic() + (timeout_ms / 1000)

        while time.monotonic() < deadline:
            if download_result["done"]:
                output_path = Path(download_result["path"])

                if output_path.exists() and output_path.stat().st_size > 0:
                    print(f"    File tersimpan: {output_path}")
                    return output_path

            stable_file = find_new_stable_file(
                download_dir=download_dir,
                before_snapshot=before_snapshot,
                last_sizes=last_sizes,
                stable_since=stable_since,
                stable_seconds=2.0,
            )

            if stable_file is not None:
                print(f"    Native file download selesai: {stable_file}")
                return stable_file

            page.wait_for_timeout(500)

        raise RuntimeError(
            "Gagal download CSV: file belum terdeteksi selesai sampai timeout. "
            f"Timeout={timeout_ms / 1000:.0f} detik. "
            f"Last capture error: {download_result['error']}"
        )

    finally:
        try:
            page.remove_listener("download", handle_download)
        except Exception:
            pass

        try:
            page.remove_listener("response", handle_response)
        except Exception:
            pass


def process_row(
    page,
    row: InputRow,
    download_dir: Path,
    timeout_ms: int,
) -> RowResult:
    try:
        downloaded_path = download_csv_from_url(
            page=page,
            full_url=row.full_url,
            download_dir=download_dir,
            timeout_ms=timeout_ms,
        )

        return RowResult(
            row=row,
            status=STATUS_SUCCESS,
            output_path=str(downloaded_path),
            actual_filename=Path(downloaded_path).name,
        )

    except Exception as exc:
        status = STATUS_FAILED_NO_DATA if isinstance(exc, NoDataForFilterError) else STATUS_FAILED

        return RowResult(
            row=row,
            status=status,
            error=str(exc),
        )


def save_error_screenshot(page, row_number: int) -> Path | None:
    screenshot_dir = Path("debug_screenshots")
    screenshot_dir.mkdir(parents=True, exist_ok=True)

    screenshot_path = screenshot_dir / f"row_{row_number}.png"

    try:
        page.screenshot(path=screenshot_path, full_page=True)
        return screenshot_path
    except Exception:
        return None


def get_sync_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Modul 'playwright' belum terpasang di Python environment ini. "
            "Install dependency lebih dulu sebelum menjalankan batch download."
        ) from exc

    return sync_playwright


# ============================================================
# BATCH RUNNER
# ============================================================

def run_batch(args: argparse.Namespace) -> int:
    input_csv = Path(args.input_csv)
    download_dir = Path(args.download_dir)
    state_file = Path(args.state_file)
    log_file = Path(args.log_file)

    rows = apply_row_window(
        rows=load_input_rows(input_csv),
        start_row=args.start_row,
        limit=args.limit,
    )
    state = load_state(state_file)

    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"Input CSV      : {input_csv}")
    print(f"Download dir   : {download_dir}")
    print(f"State file     : {state_file}")
    print(f"Log file       : {log_file}")
    print(f"Start row      : {args.start_row or '-'}")
    print(f"Total URL      : {len(rows)}")
    print(f"Cooldown       : {args.cooldown} detik")
    print(f"Timeout        : {args.timeout} detik")
    print(f"Headless       : {args.headless}")
    print(f"Force          : {args.force}")
    print("")

    stats = RunStats()
    sync_playwright = get_sync_playwright()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=args.headless,
            slow_mo=args.slow_mo,
        )

        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1440, "height": 1000},
            locale="id-ID",
            timezone_id="Asia/Jakarta",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        page = context.new_page()
        page.set_default_timeout(args.timeout * 1000)

        configure_chromium_download_dir(
            context=context,
            page=page,
            download_dir=download_dir,
        )

        try:
            for index, row in enumerate(rows, start=1):
                key = row.full_url

                print(f"[{index}/{len(rows)}] Row={row.row_number}")

                if is_completed_in_state(state, key) and not args.force:
                    print("    SKIP: sudah selesai di state.")
                    stats.skipped += 1
                    continue

                try:
                    result = process_row(
                        page=page,
                        row=row,
                        download_dir=download_dir,
                        timeout_ms=args.timeout * 1000,
                    )

                    persist_result(
                        state_file=state_file,
                        log_file=log_file,
                        state=state,
                        result=result,
                    )
                    stats.record_result(result)

                    if result.status == STATUS_SUCCESS:
                        print(f"    SUCCESS: {result.output_path}")
                    elif result.status == STATUS_FAILED_NO_DATA:
                        print("    NO DATA: tidak ada data untuk filter URL ini.")
                    else:
                        print(f"    FAILED: {result.error}")

                except Exception as exc:
                    result = RowResult(
                        row=row,
                        status=STATUS_FAILED,
                        error=f"Gagal mencatat hasil row: {exc}",
                    )
                    stats.record_result(result)
                    print(f"    FAILED: {result.error}")

                if result.status != STATUS_SUCCESS and args.screenshot_on_error:
                    screenshot_path = save_error_screenshot(page, row.row_number)
                    if screenshot_path is not None:
                        print(f"    Screenshot: {screenshot_path}")

                time.sleep(args.cooldown)

        finally:
            context.close()
            browser.close()

    print("")
    print("Summary")
    print(f"  Success : {stats.success}")
    print(f"  Failed  : {stats.failed}")
    print(f"  No data : {stats.no_data}")
    print(f"  Skipped : {stats.skipped}")
    print(f"  Log     : {log_file}")
    print(f"  State   : {state_file}")

    return 0 if stats.failed == 0 else 1


# ============================================================
# ARGPARSE
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Batch download CSV INAProc dari daftar full_url. "
            "File disimpan memakai nama bawaan website, tanpa rename. "
            "Jika muncul pesan no-data, URL dicatat sebagai failed_no_data."
        )
    )

    parser.add_argument(
        "--input-csv",
        default=DEFAULT_INPUT_CSV,
        help=f"Path CSV input. Default: {DEFAULT_INPUT_CSV}",
    )

    parser.add_argument(
        "--download-dir",
        default=DEFAULT_DOWNLOAD_DIR,
        help=f"Folder output download. Default: {DEFAULT_DOWNLOAD_DIR}",
    )

    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
        help=f"File state JSON untuk resume. Default: {DEFAULT_STATE_FILE}",
    )

    parser.add_argument(
        "--log-file",
        default=DEFAULT_LOG_FILE,
        help=f"File log CSV. Default: {DEFAULT_LOG_FILE}",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Jumlah URL yang mau diproses. Opsional. Jika tidak diisi, proses semua URL.",
    )

    parser.add_argument(
        "--start-row",
        type=int,
        default=None,
        help=(
            "Mulai proses dari nomor row CSV tertentu. "
            "Dihitung dari baris data pertama setelah header sebagai row 1."
        ),
    )

    parser.add_argument(
        "--cooldown",
        type=float,
        default=0.5,
        help="Cooldown antar URL dalam detik. Default: 0.5",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=900,
        help="Timeout per URL/download dalam detik. Default: 900",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        help="Jalankan browser tanpa tampilan.",
    )

    parser.add_argument(
        "--slow-mo",
        type=int,
        default=100,
        help="Delay Playwright per aksi dalam milidetik. Default: 100",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Proses ulang walaupun URL sudah success atau failed_no_data di state.",
    )

    parser.add_argument(
        "--screenshot-on-error",
        action="store_true",
        help="Simpan screenshot jika URL gagal.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_batch(args)


if __name__ == "__main__":
    raise SystemExit(main())
