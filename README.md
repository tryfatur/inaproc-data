# INAProc Scraping Tool

Unified CLI untuk scraping data INAProc dengan mode `list`, `detail`, dan `participant`. Tool ini dirancang untuk berjalan di VPS kecil, single-worker, resumable, dan dapat menyimpan hasil ke CSV/JSON, SQLite state, serta PostgreSQL.

## Fitur Utama

- `list`: scrape daftar paket dari `https://data.inaproc.id/realisasi`
- `detail`: scrape detail paket dari URL, kode paket, CSV, atau database
- `participant`: scrape peserta paket dari SPSE
- SQLite operational state untuk resume setelah crash/restart VPS
- Export final CSV/JSON dari SQLite state
- Insert/update PostgreSQL untuk hasil `list`
- Update PostgreSQL untuk hasil `detail`
- Insert PostgreSQL untuk hasil `participant`
- Custom URL parameter `--sumber` dan `--jenis-klpd`
- Cleansing nilai Rupiah untuk `total_nilai` dan `nilai_pdn`
- Browser restart berkala, cooldown periodik, adaptive cooldown, dan resource blocking
- Debug artifact saat failure: screenshot dan HTML

---

## Struktur Folder

Tool membuat folder berikut secara otomatis:

```text
logs/
debug/screenshots/
debug/html/
debug/traces/
output/
state/
```

Fungsi folder:

```text
logs/              Log eksekusi
debug/screenshots/ Screenshot saat error
debug/html/        HTML halaman saat error
output/            CSV/JSON hasil export
state/             SQLite state untuk resume
```

---

## Requirements

Install dependency Python:

```bash
pip install -r requirements.txt
```

Isi minimal `requirements.txt`:

```txt
playwright
psycopg2-binary
```

Install browser Chromium untuk Playwright:

```bash
python -m playwright install chromium
```

Cek dependency:

```bash
python -c "import playwright; import psycopg2; print('OK')"
```

---

## Konfigurasi `.env`

Gunakan format `KEY=value`, tanpa `export`.

```env
INAPROC_DB_HOST=localhost
INAPROC_DB_PORT=5432
INAPROC_DB_NAME=inaproc
INAPROC_DB_USER=tryfatur
INAPROC_DB_PASSWORD=xxx
INAPROC_DB_SCHEMA=tender
INAPROC_DB_DETAIL_TABLE=details
INAPROC_DB_PARTICIPANT_TABLE=participant
INAPROC_DB_KEY_FIELD=kode_paket

INAPROC_BLOCK_RESOURCES=image,media,font
INAPROC_BLOCK_URL_KEYWORDS=googletagmanager,google-analytics,analytics,doubleclick,adservice,facebook,hotjar,clarity,segment
```

Catatan:

- Script membaca `.env` otomatis dari direktori kerja.
- Jika password mengandung karakter khusus, boleh gunakan kutip: `INAPROC_DB_PASSWORD='password'`.
- Jika ingin menggunakan `export`, jalankan langsung di shell, bukan di file `.env` yang dibaca loader sederhana.

---

## Struktur Database PostgreSQL

Default konfigurasi:

```text
Database              : inaproc
Schema                : tender
Tabel detail paket    : details
Tabel peserta         : participant
Key field             : kode_paket
```

### Tabel `tender.details`

Field yang umum digunakan untuk hasil `list`:

```text
no
kode_paket
kode_paket_url
nama_instansi
nama_satuan_kerja
kode_rup
tahun_anggaran
sumber_transaksi
sumber_dana
nama_penyedia
metode_pengadaan
jenis_pengadaan
nama_paket
status_paket
total_nilai
nilai_pdn
_scraped_page
_page_label
```

Field yang umum di-update dari hasil `detail`:

```text
cara_pembayaran
instansi
kualifikasi_usaha
lokasi_pekerjaan
metode_evaluasi
nilai_hps
nilai_pagu
satuan_kerja
tanggal_tender
tanggal_tender_mulai
tanggal_tender_selesai
durasi_tender
```

### Tabel `tender.participant`

Field yang umum digunakan:

```text
nama_peserta
npwp
harga_penawaran
harga_terkoreksi
kode_paket
```

---

## Command Umum

```bash
python inaproc_tool.py --help
python inaproc_tool.py list --help
python inaproc_tool.py detail --help
python inaproc_tool.py participant --help
```

---

# 1. Mode `list`

Mode `list` digunakan untuk mengambil daftar paket dari halaman realisasi INAProc.

## 1.1 Scrape list ke CSV/SQLite state

Tender:

```bash
python inaproc_tool.py list \
  --year 2025 \
  --jenis-klpd 3 4 5 \
  --sumber Tender \
  --start-page 1 \
  --end-page 10 \
  --state-db state/list_tender_2025.sqlite \
  --output output/list_tender_2025.csv \
  --failed output/list_tender_2025_failed.csv \
  --headless
```

Non Tender:

```bash
python inaproc_tool.py list \
  --year 2025 \
  --jenis-klpd 1 2 \
  --sumber "Non Tender" \
  --start-page 1 \
  --end-page 10 \
  --state-db state/list_nontender_2025.sqlite \
  --output output/list_nontender_2025.csv \
  --failed output/list_nontender_2025_failed.csv \
  --headless
```

Catatan `--sumber`:

- Jika mengandung spasi, gunakan tanda kutip.
- Contoh benar: `--sumber "Non Tender"`.
- Jangan menulis `Non+Tender` di terminal. Encoding URL ditangani oleh script.

## 1.2 Scrape list dan insert/update ke PostgreSQL

```bash
python inaproc_tool.py list \
  --year 2025 \
  --jenis-klpd 1 2 \
  --sumber "Non Tender" \
  --start-page 1 \
  --end-page 2 \
  --insert-db \
  --state-db state/list_nontender_2025_db.sqlite \
  --output output/list_nontender_2025_db.csv \
  --failed output/list_nontender_2025_db_failed.csv \
  --headless
```

Dengan DB config eksplisit:

```bash
python inaproc_tool.py list \
  --year 2025 \
  --jenis-klpd 1 2 \
  --sumber "Non Tender" \
  --start-page 1 \
  --end-page 2 \
  --insert-db \
  --db-host localhost \
  --db-port 5432 \
  --db-name inaproc \
  --db-user tryfatur \
  --db-password password \
  --db-schema tender \
  --db-detail-table details \
  --db-key-field kode_paket \
  --state-db state/list_nontender_2025_db.sqlite \
  --output output/list_nontender_2025_db.csv \
  --failed output/list_nontender_2025_db_failed.csv \
  --headless
```

Perilaku `--insert-db`:

```text
1. Scrape data list per halaman
2. Normalize field hasil scrape
3. Ambil hanya field yang cocok dengan struktur tabel tender.details
4. Cleansing Total Nilai ke integer
5. Cleansing Nilai PDN / Nilai PDN (Rp) ke integer
6. UPDATE berdasarkan kode_paket jika row sudah ada
7. INSERT jika kode_paket belum ada
8. Simpan hasil ke SQLite state
9. Export CSV/JSON final dari SQLite state
```

Contoh output sukses:

```text
DB list sync: inserted=50, updated=0, skipped=0, failed=0
DB list sync: inserted=45, updated=5, skipped=0, failed=0
DB inserted total: 95
DB updated total: 5
DB skipped total: 0
DB failed total: 0
```

## 1.3 Export ulang dari SQLite state

```bash
python inaproc_tool.py list \
  --export-only \
  --state-db state/list_nontender_2025_db.sqlite \
  --output output/list_nontender_2025_export.csv \
  --failed output/list_nontender_2025_failed_export.csv
```

Catatan:

- `--export-only` tidak melakukan scraping ulang.
- `--insert-db` tidak berjalan jika tidak ada pending item.
- Jika ingin force re-run, gunakan state DB baru atau hapus file SQLite state lama.

---

# 2. Mode `detail`

Mode `detail` membuka halaman detail paket dan mengambil tabel dengan header:

```text
No | Deskripsi | Detail
```

Kemudian tabel ditranspose:

```text
Kolom Deskripsi -> header field
Kolom Detail    -> value field
```

## 2.1 Detail dari DB default

Jika tidak memberikan sumber, mode `detail` mengambil `kode_paket` dari database untuk row yang field `instansi` masih kosong/null.

```bash
python inaproc_tool.py detail \
  --update-db \
  --limit 100 \
  --state-db state/detail_update.sqlite \
  --output output/detail_final.csv \
  --failed output/detail_failed.csv \
  --headless
```

Dengan start row:

```bash
python inaproc_tool.py detail \
  --update-db \
  --start 1001 \
  --limit 100 \
  --state-db state/detail_batch_1001.sqlite \
  --output output/detail_batch_1001.csv \
  --failed output/detail_batch_1001_failed.csv \
  --headless
```

## 2.2 Detail dari kode paket langsung

```bash
python inaproc_tool.py detail \
  --kode-paket 10064298000 \
  --update-db \
  --state-db state/detail_single.sqlite \
  --output output/detail_single.csv \
  --failed output/detail_single_failed.csv \
  --headless
```

## 2.3 Detail dari CSV

CSV minimal punya kolom URL atau Kode Paket.

```bash
python inaproc_tool.py detail \
  --input-csv output/list_final.csv \
  --kode-column "Kode Paket" \
  --update-db \
  --state-db state/detail_from_csv.sqlite \
  --output output/detail_from_csv.csv \
  --failed output/detail_from_csv_failed.csv \
  --headless
```

Jika pakai kolom URL:

```bash
python inaproc_tool.py detail \
  --input-csv output/list_final.csv \
  --url-column "Kode Paket_url" \
  --update-db \
  --state-db state/detail_from_url.sqlite \
  --output output/detail_from_url.csv \
  --failed output/detail_from_url_failed.csv \
  --headless
```

## 2.4 Mapping field detail ke DB

Default mapping:

```text
Cara Pembayaran        -> cara_pembayaran
Instansi               -> instansi
Kualifikasi Usaha      -> kualifikasi_usaha
Lokasi Pekerjaan       -> lokasi_pekerjaan
Metode Evaluasi        -> metode_evaluasi
Nilai HPS              -> nilai_hps
Nilai Pagu             -> nilai_pagu
Satuan Kerja           -> satuan_kerja
Tanggal Tender         -> tanggal_tender
tanggal_tender_mulai   -> tanggal_tender_mulai
tanggal_tender_selesai -> tanggal_tender_selesai
durasi_tender          -> durasi_tender
```

Custom mapping dapat dibuat dalam JSON:

```json
{
  "Cara Pembayaran": "cara_pembayaran",
  "Instansi": "instansi",
  "Tanggal Tender": "tanggal_tender"
}
```

Lalu jalankan:

```bash
python inaproc_tool.py detail \
  --kode-paket 10064298000 \
  --update-db \
  --field-map field_map_detail.json \
  --state-db state/detail_custom_map.sqlite \
  --output output/detail_custom_map.csv \
  --failed output/detail_custom_map_failed.csv
```

---

# 3. Mode `participant`

Mode `participant` digunakan untuk mengambil peserta tender dari SPSE.

## 3.1 Participant dari CSV

CSV wajib punya kolom yang bisa dikenali sebagai:

```text
Nama Instansi
Kode Paket
```

Command:

```bash
python inaproc_tool.py participant \
  --input-csv output/list_tender_2025.csv \
  --year 2025 \
  --state-db state/participant_2025.sqlite \
  --output output/participant_2025.csv \
  --failed output/participant_2025_failed.csv \
  --headless
```

## 3.2 Participant dari DB

```bash
python inaproc_tool.py participant \
  --from-db \
  --nama-instansi "KAB. GIANYAR" \
  --year 2025 \
  --limit 100 \
  --state-db state/participant_gianyar_2025.sqlite \
  --output output/participant_gianyar_2025.csv \
  --failed output/participant_gianyar_2025_failed.csv \
  --headless
```

## 3.3 Insert participant ke PostgreSQL

```bash
python inaproc_tool.py participant \
  --from-db \
  --nama-instansi "KAB. GIANYAR" \
  --year 2025 \
  --limit 100 \
  --insert-db \
  --state-db state/participant_gianyar_2025_db.sqlite \
  --output output/participant_gianyar_2025_db.csv \
  --failed output/participant_gianyar_2025_db_failed.csv \
  --headless
```

---

## Operational State dan Resume

Tool memakai SQLite state DB sebagai operational storage.

Contoh:

```bash
--state-db state/list_nontender_2025.sqlite
```

Status item:

```text
pending
success
failed
```

Perilaku penting:

- Item `success` tidak diproses ulang pada state DB yang sama.
- Item `failed` dicoba ulang secara default.
- `--no-retry-failed` membuat item failed tidak dicoba ulang.
- `--reset-failed` mengubah failed menjadi pending.
- Untuk menjalankan ulang item success, gunakan state DB baru atau hapus state DB lama.

Cek state DB:

```bash
sqlite3 state/list_nontender_2025.sqlite
```

Query:

```sql
SELECT status, COUNT(*)
FROM scrape_items
GROUP BY status;
```

Lihat item failed:

```sql
SELECT item_key, attempts, failed_step, last_error
FROM scrape_items
WHERE status = 'failed'
LIMIT 20;
```

---

## Resource Blocking

Default resource yang diblok:

```text
image,font,media,stylesheet
```

Override lewat CLI:

```bash
--block-resources image,media,font
```

Override lewat `.env`:

```env
INAPROC_BLOCK_RESOURCES=image,media,font
```

Default keyword URL yang diblok:

```text
googletagmanager,google-analytics,analytics,doubleclick,adservice,facebook,hotjar,clarity,segment,adsystem,beacon
```

Override:

```bash
--block-url-keywords googletagmanager,google-analytics,analytics,doubleclick
```

---

## Browser dan Cooldown

Argumen umum:

```text
--headless                  Jalankan browser tanpa UI
--headful                   Tampilkan browser
--timeout                   Timeout Playwright dalam milidetik
--delay                     Delay antar item/halaman
--restart-browser-every     Restart Chromium setiap N item/page
--cooldown-every            Cooldown periodik setiap N item/page
--cooldown-min              Minimum cooldown periodik
--cooldown-max              Maksimum cooldown periodik
--adaptive-error-threshold  Mulai adaptive cooldown setelah N error beruntun
--adaptive-cooldown-base    Base adaptive cooldown
--adaptive-cooldown-max     Maksimum adaptive cooldown
```

Contoh VPS kecil:

```bash
python inaproc_tool.py list \
  --year 2025 \
  --jenis-klpd 1 2 \
  --sumber "Non Tender" \
  --start-page 1 \
  --end-page 100 \
  --insert-db \
  --restart-browser-every 500 \
  --cooldown-every 1000 \
  --cooldown-min 5 \
  --cooldown-max 10 \
  --delay 0 \
  --headless
```

---

## Tracing Database

Cek koneksi PostgreSQL:

```bash
psql -U tryfatur -d inaproc
```

Query:

```sql
SELECT current_database(), current_user, current_schema();
```

Cek total row:

```sql
SELECT COUNT(*)
FROM tender.details;
```

Cek data terbaru:

```sql
SELECT
    kode_paket,
    tahun_anggaran,
    sumber_transaksi,
    nama_instansi,
    total_nilai,
    nilai_pdn,
    _scraped_page,
    _page_label
FROM tender.details
ORDER BY ctid DESC
LIMIT 30;
```

Cek Non Tender:

```sql
SELECT
    kode_paket,
    tahun_anggaran,
    sumber_transaksi,
    nama_instansi,
    total_nilai,
    nilai_pdn
FROM tender.details
WHERE tahun_anggaran = '2025'
  AND sumber_transaksi ILIKE '%Non%'
LIMIT 30;
```

Cek variasi sumber transaksi:

```sql
SELECT DISTINCT sumber_transaksi
FROM tender.details
WHERE tahun_anggaran = '2025'
ORDER BY sumber_transaksi;
```

---

## Troubleshooting

### 1. `Total list page pending: 0`

Semua page pada SQLite state tersebut sudah berstatus `success`.

Solusi:

```bash
rm state/nama_state.sqlite
```

atau gunakan state baru:

```bash
--state-db state/run_baru.sqlite
```

### 2. `Cannot operate on a closed database`

SQLite `store.close()` terpanggil sebelum proses export atau summary selesai.

Pastikan `store.close()` hanya ada di `finally` paling bawah function command, setelah:

```text
export_mode_results()
export_failed()
store.count_by_status()
```

### 3. `argument should be a str or os.PathLike object, not NoneType`

Ada `Path(None)`, biasanya dari `args.state_db`, `args.output`, atau `args.failed`.

Solusi:

```bash
--state-db state/run.sqlite \
--output output/result.csv \
--failed output/failed.csv
```

### 4. `.env` tidak terbaca

Gunakan format tanpa `export`:

```env
INAPROC_DB_HOST=localhost
```

Bukan:

```env
export INAPROC_DB_HOST=localhost
```

### 5. Data tidak terlihat di PostgreSQL

Cek hal berikut:

```text
1. Apakah command memakai --insert-db?
2. Apakah Total list page pending > 0?
3. Apakah output menampilkan DB list sync?
4. Apakah database/schema/table yang dicek sama?
5. Apakah query pengecekan terlalu ketat?
```

Gunakan query longgar:

```sql
SELECT *
FROM tender.details
ORDER BY ctid DESC
LIMIT 20;
```

### 6. `Non Tender` error di CLI

Gunakan tanda kutip:

```bash
--sumber "Non Tender"
```

---

## Estimasi Waktu Scraping

Rumus:

```text
total_waktu = rata_rata_detik_per_halaman × jumlah_halaman
```

Contoh hasil empiris:

```text
1.37 detik per halaman
```

Estimasi:

```text
1.000 halaman   ≈ 22 menit 50 detik
35.493 halaman  ≈ 13.5 jam
271.488 halaman ≈ 4.3 hari
```

Estimasi aktual dapat berubah karena koneksi, response server, timeout, retry, dan performa VPS.

---

## Rekomendasi Operasional

1. Gunakan state DB berbeda per batch besar.
2. Jangan overwrite state DB lama kecuali sengaja reset.
3. Simpan output dan failed CSV per batch.
4. Untuk VPS kecil, tetap single-worker.
5. Validasi 1–2 page dulu sebelum menjalankan ribuan halaman.
6. Gunakan `--insert-db` hanya setelah mapping field dicek.
7. Cek PostgreSQL dengan query tanpa filter ketat saat tracing awal.
8. Simpan log dan state DB sebagai bukti proses.

---

## Workflow Lengkap

### Step 1 — Scrape list Non Tender dan simpan ke DB

```bash
python inaproc_tool.py list \
  --year 2025 \
  --jenis-klpd 1 2 \
  --sumber "Non Tender" \
  --start-page 1 \
  --end-page 100 \
  --insert-db \
  --state-db state/list_nontender_2025_p1_100.sqlite \
  --output output/list_nontender_2025_p1_100.csv \
  --failed output/list_nontender_2025_p1_100_failed.csv \
  --headless
```

### Step 2 — Scrape detail untuk row yang `instansi` masih kosong

```bash
python inaproc_tool.py detail \
  --update-db \
  --start 1 \
  --limit 1000 \
  --state-db state/detail_batch_001.sqlite \
  --output output/detail_batch_001.csv \
  --failed output/detail_batch_001_failed.csv \
  --headless
```

### Step 3 — Scrape participant dari DB

```bash
python inaproc_tool.py participant \
  --from-db \
  --nama-instansi "KAB. GIANYAR" \
  --year 2025 \
  --limit 100 \
  --insert-db \
  --state-db state/participant_gianyar_2025.sqlite \
  --output output/participant_gianyar_2025.csv \
  --failed output/participant_gianyar_2025_failed.csv \
  --headless
```

---

## Catatan Patch Terakhir

Fitur yang perlu ada di versi terbaru:

```text
list --jenis-klpd
list --sumber
list --insert-db
add_db_args(list_parser)
DEFAULT_LIST_DB_FIELD_MAP
normalize_list_record_for_db()
insert_or_update_list_records_to_db()
cleansing total_nilai
cleansing nilai_pdn
defensive default untuk state_db/output/failed
store.close() hanya di finally paling bawah
```

Jika salah satu fitur belum ada, patch script belum lengkap.