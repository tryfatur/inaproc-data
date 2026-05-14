# INAProc Scraper Tool

Tool CLI untuk scraping data INAProc/SPSE dengan desain VPS-friendly, single worker, resumable, dan berbasis batch.

Subcommand utama:

- `list`: scrape daftar paket dari `data.inaproc.id/realisasi`
- `detail`: scrape detail tender, transform nilai/tanggal, lalu opsional update PostgreSQL
- `participant`: scrape peserta tender dari SPSE, lalu opsional insert PostgreSQL

---

## 1. Instalasi

```bash
python -m venv .venv
source .venv/bin/activate

pip install -r requirements_inaproc_tool_resumable.txt
playwright install chromium
```

Untuk VPS Linux minimal:

```bash
playwright install-deps chromium
```

Jika dependency OS tidak bisa dipasang lengkap, minimal biasanya perlu:

```bash
sudo apt install -y \
  libnss3 \
  libatk-bridge2.0-0 \
  libgtk-3-0 \
  libgbm1
```

---

## 2. Konfigurasi `.env`

Buat file `.env` di folder yang sama dengan `inaproc_tool.py`.

```env
INAPROC_DB_HOST=localhost
INAPROC_DB_PORT=5432
INAPROC_DB_NAME=inaproc
INAPROC_DB_USER=tryfatur
INAPROC_DB_PASSWORD=isi_password_di_sini

INAPROC_DB_SCHEMA=tender
INAPROC_DB_DETAIL_TABLE=details
INAPROC_DB_PARTICIPANT_TABLE=participant
INAPROC_DB_KEY_FIELD=kode_paket

INAPROC_BLOCK_RESOURCES=image,font,media,stylesheet
INAPROC_BLOCK_URL_KEYWORDS=googletagmanager,google-analytics,analytics,doubleclick,adservice,facebook,hotjar,clarity,segment,adsystem,beacon
```

Script membaca `.env` otomatis apabila sudah dipatch dengan loader `.env`. Jika belum, load manual:

```bash
set -a
source .env
set +a
```

---

## 3. Struktur tabel DB yang diasumsikan

### Tabel detail

Default:

```text
tender.details
```

Kolom penting:

```text
kode_paket
batch
instansi
nama_instansi
nilai_hps
nilai_pagu
tanggal_tender_mulai
tanggal_tender_selesai
durasi_tender
```

Kolom tambahan untuk hasil transform tanggal:

```sql
ALTER TABLE tender.details
ADD COLUMN IF NOT EXISTS tanggal_tender_mulai date,
ADD COLUMN IF NOT EXISTS tanggal_tender_selesai date,
ADD COLUMN IF NOT EXISTS durasi_tender integer;
```

### Tabel participant

Default:

```text
tender.participant
```

Kolom default insert participant:

```text
nama_peserta
npwp
harga_penawaran
harga_terkoreksi
kode_paket
```

---

## 4. Prinsip eksekusi terbaru

### Batch sebagai pembatas utama

Untuk subcommand `detail` dan `participant`, `batch` adalah pembatas utama scraping.

Flow `detail`:

```text
batch tertentu
↓
ambil row dari tabel detail dalam batch tersebut
↓
filter instansi NULL/kosong
↓
ambil kode_paket
↓
scrape detail tender
↓
update DB berdasarkan kode_paket
```

Flow `participant`:

```text
batch tertentu
↓
ambil row dari tabel detail dalam batch tersebut
↓
ambil kode_paket + nama_instansi
↓
scrape peserta tender dari SPSE
↓
insert participant ke DB jika --insert-db aktif
```

### State DB otomatis per batch

Jika `--state-db` tidak diisi, script membuat state otomatis:

```text
state/detail_batch_<batch>.sqlite
state/participant_batch_<batch>.sqlite
```

Contoh:

```bash
--batch 1
```

State otomatis:

```text
state/detail_batch_1.sqlite
state/participant_batch_1.sqlite
```

State ini dipakai untuk resume setelah crash/restart VPS.

---

## 5. Resource mode VPS

Default scraping memakai:

- single worker only
- Chromium headless
- block `image,font,media,stylesheet`
- restart browser berkala
- cooldown periodik
- adaptive cooldown saat timeout/error berturut
- debug artifact hanya saat failure

Argumen penting:

```bash
--restart-browser-every 750
--cooldown-every 1000
--cooldown-min 5
--cooldown-max 10
--adaptive-error-threshold 2
--adaptive-cooldown-base 5
--adaptive-cooldown-max 90
--block-resources image,font,media,stylesheet
```

---

## 6. Scrape detail tender berdasarkan batch

### Trial 10 data dalam batch

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --update-db \
  --start 1 \
  --limit 10
```

Query sumber data secara konseptual:

```sql
SELECT kode_paket
FROM tender.details
WHERE kode_paket IS NOT NULL
  AND batch = '1'
  AND NULLIF(TRIM(COALESCE(instansi::text, '')), '') IS NULL
ORDER BY kode_paket
LIMIT 10 OFFSET 0;
```

### Full run satu batch

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --update-db
```

### Resume batch setelah crash/restart

Jalankan command yang sama:

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --update-db
```

Script akan membaca:

```text
state/detail_batch_1.sqlite
```

Item dengan status `success` akan diskip.

### Export ulang hasil detail dari state

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --export-only \
  --output output/detail_batch_1.csv \
  --failed output/detail_batch_1_failed.csv
```

### Override state DB manual

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --update-db \
  --state-db state/custom_detail_batch_1.sqlite
```

---

## 7. Transform data detail tender

Saat scraping detail, script melakukan transform berikut.

### Cleansing `nilai_hps`

Contoh:

```text
Rp 367.400.000 → 367400000
```

### Cleansing `nilai_pagu`

Contoh:

```text
Rp 367.400.000 → 367400000
```

### Split `tanggal_tender`

Contoh input:

```text
28 Dec 2021 s/d 18 Jan 2022
```

Output:

```text
tanggal_tender_mulai   = 2021-12-28
tanggal_tender_selesai = 2022-01-18
durasi_tender          = 21
```

Mapping default ke DB:

```python
DEFAULT_DETAIL_DB_FIELD_MAP = {
    "Cara Pembayaran": "cara_pembayaran",
    "Instansi": "instansi",
    "Kualifikasi Usaha": "kualifikasi_usaha",
    "Lokasi Pekerjaan": "lokasi_pekerjaan",
    "Metode Evaluasi": "metode_evaluasi",
    "Nilai HPS": "nilai_hps",
    "Nilai Pagu": "nilai_pagu",
    "Satuan Kerja": "satuan_kerja",
    "tanggal_tender_mulai": "tanggal_tender_mulai",
    "tanggal_tender_selesai": "tanggal_tender_selesai",
    "durasi_tender": "durasi_tender",
}
```

---

## 8. Scrape participant berdasarkan batch

### Trial 10 data dalam batch

```bash
python inaproc_tool.py participant \
  --batch 1 \
  --insert-db \
  --year 2025 \
  --start 1 \
  --limit 10
```

Query sumber data secara konseptual:

```sql
SELECT kode_paket, nama_instansi
FROM tender.details
WHERE kode_paket IS NOT NULL
  AND batch = '1'
  AND NULLIF(TRIM(COALESCE(nama_instansi::text, '')), '') IS NOT NULL
ORDER BY kode_paket
LIMIT 10 OFFSET 0;
```

### Full run satu batch

```bash
python inaproc_tool.py participant \
  --batch 1 \
  --insert-db \
  --year 2025
```

### Resume participant setelah crash/restart

Jalankan command yang sama:

```bash
python inaproc_tool.py participant \
  --batch 1 \
  --insert-db \
  --year 2025
```

Script akan membaca:

```text
state/participant_batch_1.sqlite
```

### Export ulang hasil participant dari state

```bash
python inaproc_tool.py participant \
  --batch 1 \
  --export-only \
  --output output/participant_batch_1.csv \
  --failed output/participant_batch_1_failed.csv
```

---

## 9. Penggunaan `--start` dan `--limit`

`--start` dan `--limit` dipakai untuk trial di dalam batch.

Contoh:

```bash
--batch 1 --start 101 --limit 50
```

Artinya:

```text
ambil batch 1
mulai dari row ke-101 dalam batch tersebut
proses maksimal 50 item
```

Catatan penting:

- `--start` adalah alias dari `--start-row`.
- `--limit` sebaiknya diterapkan di dua tempat:
  - saat fetch source DB
  - saat membaca pending/failed dari SQLite state
- Untuk trial bersih, gunakan state khusus atau hapus state lama.

Contoh state trial:

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --update-db \
  --start 1 \
  --limit 10 \
  --state-db state/detail_batch_1_trial_10.sqlite
```

---

## 10. Reset dan retry

### Retry failed otomatis

Default: item `failed` ikut diproses ulang.

Untuk tidak retry failed:

```bash
--no-retry-failed
```

### Reset failed ke pending

```bash
--reset-failed
```

### Reset state total

Jika patch `--reset-state` sudah ditambahkan:

```bash
--reset-state
```

Jika belum ada, hapus file state manual:

```bash
rm state/detail_batch_1.sqlite
rm state/participant_batch_1.sqlite
```

---

## 11. Scrape daftar paket INAProc

Subcommand `list` masih bisa dipakai untuk mengambil data dari halaman realisasi INAProc.

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
python inaproc_tool.py list \
  --year 2025 \
  --start-page 1 \
  --end-page 1 \
  --headful
```

---

## 12. Menjalankan di VPS dengan `tmux`

Buat session:

```bash
tmux new -s inaproc
```

Jalankan proses:

```bash
cd /home/inaproc-data
source .venv/bin/activate

python inaproc_tool.py detail \
  --batch 1 \
  --update-db
```

Detach:

```text
CTRL + B, lalu D
```

Masuk lagi:

```bash
tmux attach -t inaproc
```

Stop proses dengan aman:

```text
CTRL + C
```

---

## 13. Debug artifact

Debug artifact hanya dibuat saat failure.

Folder default:

```text
logs/
debug/screenshots/
debug/html/
debug/traces/
```

Gunakan trace hanya saat investigasi berat:

```bash
--debug-trace
```

Trace Playwright bisa besar, jadi tidak direkomendasikan untuk full run di VPS kecil.

---

## 14. Validasi setelah patch

Setelah edit script:

```bash
python -m py_compile inaproc_tool.py
python inaproc_tool.py --help
python inaproc_tool.py detail --help
python inaproc_tool.py participant --help
```

Trial detail:

```bash
python inaproc_tool.py detail \
  --batch 1 \
  --update-db \
  --start 1 \
  --limit 10
```

Trial participant:

```bash
python inaproc_tool.py participant \
  --batch 1 \
  --insert-db \
  --year 2025 \
  --start 1 \
  --limit 10
```

---

## 15. Ringkasan command utama

Detail batch:

```bash
python inaproc_tool.py detail --batch 1 --update-db
```

Participant batch:

```bash
python inaproc_tool.py participant --batch 1 --insert-db --year 2025
```

Export detail:

```bash
python inaproc_tool.py detail --batch 1 --export-only --output output/detail_batch_1.csv
```

Export participant:

```bash
python inaproc_tool.py participant --batch 1 --export-only --output output/participant_batch_1.csv
```