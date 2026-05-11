# INAProc Unified Tool

Tool ini menggabungkan lima script lama menjadi satu CLI:

- `list`: scrape daftar paket dari `data.inaproc.id/realisasi`
- `detail`: scrape halaman detail paket dari URL/kode paket/DB, backup ke CSV/JSONL, opsional update PostgreSQL
- `participant`: scrape peserta paket dari SPSE, output CSV, opsional insert PostgreSQL

## Instalasi

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements_inaproc_tool.txt
playwright install chromium
```

## Konfigurasi DB

Gunakan environment variable agar kredensial tidak tertanam di kode:

```bash
export INAPROC_DB_HOST=localhost
export INAPROC_DB_PORT=5432
export INAPROC_DB_NAME=inaproc
export INAPROC_DB_USER=username
export INAPROC_DB_PASSWORD='isi_password_di_sini'
```

Schema dan tabel default:

- schema: `tender`
- detail table: `details`
- participant table: `participant`
- key field: `kode_paket`

Semua bisa dioverride dengan argument `--db-schema`, `--db-detail-table`, `--db-participant-table`, dan `--db-key-field`.

## 1. Scrape daftar paket

```bash
python inaproc_tool.py list \
  --year 2025 \
  --start-page 1 \
  --end-page 5 \
  --entries-per-page 50 \
  --output output/paket_2025.csv
```

Untuk melihat browser:

```bash
python inaproc_tool.py list --year 2025 --start-page 1 --end-page 1 --headful
```

## 2. Scrape detail paket dari CSV

Jika CSV punya kolom `Kode Paket`:

```bash
python inaproc_tool.py detail \
  --input-csv output/paket_2025.csv \
  --kode-column "Kode Paket" \
  --output output/detail_2025.csv \
  --checkpoint output/detail_2025.jsonl \
  --limit 10
```

Jika CSV punya kolom URL detail:

```bash
python inaproc_tool.py detail \
  --input-csv output/paket_2025.csv \
  --url-column "Detail_url" \
  --output output/detail_2025.csv
```

## 3. Scrape detail paket dan update PostgreSQL

```bash
python inaproc_tool.py detail \
  --from-db \
  --nama-instansi "Nama Instansi" \
  --year 2025 \
  --update-db \
  --output output/detail_db_backup.csv \
  --checkpoint output/detail_db_checkpoint.jsonl
```

Mapping default hasil scrape ke kolom DB:

```json
{
  "Cara Pembayaran": "cara_pembayaran",
  "Instansi": "instansi",
  "Kualifikasi Usaha": "kualifikasi_usaha",
  "Lokasi Pekerjaan": "lokasi_pekerjaan",
  "Metode Evaluasi": "metode_evaluasi",
  "Nilai HPS": "nilai_hps",
  "Nilai Pagu": "nilai_pagu",
  "Satuan Kerja": "satuan_kerja",
  "Tanggal Tender": "tanggal_tender"
}
```

Mapping bisa diganti dengan file JSON dan argument `--field-map mapping.json`.

## 4. Scrape peserta dari CSV

CSV wajib memiliki kolom `Nama Instansi` dan `Kode Paket`.

```bash
python inaproc_tool.py participant \
  --input-csv output/paket_2025.csv \
  --year 2025 \
  --output output/peserta_2025.csv \
  --failed output/peserta_2025_failed.csv \
  --limit 10
```

Mode peserta default `headful` untuk membantu halaman SPSE yang dinamis. Tambahkan `--headless` jika ingin tanpa UI.

## 5. Scrape peserta dari DB dan insert hasil ke PostgreSQL

```bash
python inaproc_tool.py participant \
  --from-db \
  --nama-instansi "Nama Instansi" \
  --year 2025 \
  --insert-db \
  --output output/peserta_db_backup.csv \
  --failed output/peserta_db_failed.csv
```

## Output debug

Saat gagal, tool menyimpan debug ke:

- `logs/inaproc_tool_<timestamp>.log`
- `debug/screenshots/`
- `debug/html/`
- `debug/traces/` jika `--debug-trace` dipakai

## Catatan refactoring

- Tidak ada kredensial DB hard-coded di script baru.
- Fungsi CSV, JSONL checkpoint, logging, debug, dan DB dibuat reusable.
- Semua operasi kini berada di satu entry point `inaproc_tool.py` dengan subcommand.
- Script memakai Playwright sync agar alur list/detail/peserta konsisten.
