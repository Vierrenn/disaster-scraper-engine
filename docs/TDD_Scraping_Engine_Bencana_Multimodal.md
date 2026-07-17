# Technical Design Document — Scraping Engine untuk Dataset Berita Bencana Multimodal

**Status:** Draft v1.0 · Design Phase (pre-implementation)
**Owner:** Farhan
**Reviewer role author:** Principal Software Architect / Senior Data Engineer
**Scope dokumen:** Fondasi Scraping Engine + jalur ETL sampai dataset final. Bukan pemodelan ML.
**Prinsip pemandu:** _Decouple acquisition from extraction. Store raw immutably. Make everything idempotent, resumable, and reproducible._

> Catatan pembaca: dokumen ini sengaja panjang dan tidak diringkas, sesuai permintaan. Setiap keputusan besar disertai trade-off dan alternatif. Bagian yang saling berkesinambungan ditandai referensi silang antar-section.

---

## Ringkasan Keputusan Arsitektural (TL;DR untuk reviewer)

Sepuluh keputusan inti yang menjadi tulang punggung desain:

1. **Pisahkan acquisition dari extraction.** Fetcher hanya bertugas mengambil dan menyimpan raw HTML + raw image bytes secara _immutable_. Parsing dilakukan di stage terpisah yang bisa diulang tanpa re-crawl.
2. **Medallion architecture (Bronze → Silver → Gold).** Raw HTML/bytes (Bronze) → parsed & normalized records (Silver) → dataset final tervalidasi (Gold). Ini memetakan diagram pipeline-mu ke pola industri yang matang.
3. **Config-driven adapters.** Mayoritas sumber didefinisikan lewat selector di file konfigurasi (YAML), bukan kode. Kode custom hanya untuk sumber yang benar-benar sulit.
4. **Content-addressed storage.** ID artikel & nama file gambar berbasis hash konten → dedup natural + idempotency gratis.
5. **URL Frontier dengan politeness per-domain.** Antrian crawl memisahkan penjadwalan per host agar rate limiting adil dan patuh.
6. **Jangan reinvent the wheel.** Scrapy sudah menyediakan scheduler, frontier, throttling, retry, dan item pipeline. Untuk kasus ini, ini rekomendasi utama (dengan trade-off dibahas di §14).
7. **Pisahkan OLTP dari OLAP.** State crawl (transaksional) di PostgreSQL/SQLite; dataset analitik di Parquet + DuckDB.
8. **Idempotent & incremental by default.** Re-run tidak menduplikasi data; crawl bisa dilanjutkan setelah crash.
9. **Observability sejak hari pertama.** Structured logging + metrik dasar (crawl rate, success rate, parse yield per-source) bukan afterthought.
10. **Legal & etika sebagai constraint kelas satu.** Patuh `robots.txt`/ToS, hormati copyright, simpan URL untuk atribusi, dan pertimbangkan menyimpan _derived features_ alih-alih teks penuh bila dataset akan diredistribusi (lihat §13).

---

# 1. Requirement Analysis

## 1.1 Tujuan Sistem

Membangun **scraping engine yang reproducible, extensible, dan patuh hukum** untuk memproduksi **dataset berita bencana multimodal** (teks + gambar + metadata spasio-temporal) dari portal berita Indonesia, yang siap dipakai untuk klasifikasi jenis bencana, klasifikasi tingkat keparahan, ekstraksi lokasi (NER), analisis spasio-temporal, dan training model multimodal.

Kata kunci desain: **reproducible** (hasil bisa dibuat ulang), **extensible** (tambah sumber tanpa ubah core), **compliant** (patuh robots/ToS/copyright), **resumable** (tahan crash), **auditable** (setiap record punya jejak asal).

## 1.2 Scope

**In scope:**

- Keyword-driven discovery (pencarian artikel relevan per keyword bencana).
- Multi-source crawling terhadap portal berita Indonesia (Antara, Kompas, Detik, Tempo, CNN Indonesia, Liputan6, Kumparan, Tribun, Radar Daerah, portal BPBD).
- Acquisition (fetch + simpan raw HTML immutable), image download, parsing/field extraction, image processing, validation, cleaning, deduplication, normalization.
- ETL sampai dataset final (Parquet/JSON/DB) beserta katalog metadata.
- Data quality gates & observability.

**Non-scope (eksplisit):**

- Pemodelan/training ML, arsitektur fusion, dan model NER itu sendiri (engine ini _memberi makan_ mereka, bukan membangunnya).
- **Scraping media sosial (Twitter/X, Instagram).** Meskipun muncul di Image 2, ini _out of scope_ untuk engine ini karena: (a) mayoritas butuh API resmi berbayar/terbatas, (b) ToS-nya melarang scraping HTML, (c) sifat data (short text, noise tinggi) menuntut pipeline berbeda. Jika diperlukan, ini menjadi _ingestion adapter_ terpisah di fase berikutnya, bukan bagian core sekarang.
- Real-time/streaming ingestion. Sistem ini **batch-first**; streaming adalah evolusi opsional (lihat §11).
- Anotasi/labeling UI (severity, dsb.) — itu tahap downstream terpisah.
- Republikasi konten berita ke publik.

## 1.3 Functional Requirements

| ID    | Requirement                                                                                                                                     |
| ----- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| FR-1  | Menerima daftar keyword + daftar sumber, lalu menemukan URL artikel relevan (discovery).                                                        |
| FR-2  | Mengambil (fetch) halaman artikel dan menyimpan raw HTML secara immutable.                                                                      |
| FR-3  | Mengunduh gambar utama (dan gambar konten bila relevan) per artikel.                                                                            |
| FR-4  | Mengekstrak field terstruktur: judul, isi, ringkasan, tanggal terbit, penulis, lokasi, URL, URL gambar, jenis bencana, dampak, metadata sumber. |
| FR-5  | Menormalkan field (tanggal ISO-8601 + timezone, lokasi terstandar, whitespace, encoding).                                                       |
| FR-6  | Mendeteksi & menghapus duplikat pada level URL, artikel (near-duplicate), dan gambar (exact + near).                                            |
| FR-7  | Memvalidasi record terhadap schema & aturan kualitas; mengarantina yang gagal.                                                                  |
| FR-8  | Mendukung penambahan sumber baru **tanpa mengubah core engine** (§4).                                                                           |
| FR-9  | Crawl bersifat **incremental** (hanya artikel baru) dan **resumable** (lanjut setelah crash).                                                   |
| FR-10 | Mengekspor dataset final ke Parquet/JSON dan mendaftarkannya di katalog metadata.                                                               |
| FR-11 | Idempotent: menjalankan ulang tahap apa pun tidak menghasilkan duplikasi/side-effect ganda.                                                     |

## 1.4 Non-Functional Requirements

- **Scalability:** desain harus valid dari 1 mesin (skripsi) hingga terdistribusi (jutaan artikel, puluhan juta gambar) tanpa rewrite (§11).
- **Politeness & compliance:** patuh `robots.txt`, rate limit per-domain, User-Agent jujur, hormati ToS/copyright (§13).
- **Reproducibility:** dari raw yang tersimpan + kode versi tertentu, dataset final dapat diproduksi ulang byte-identik pada level logika.
- **Fault tolerance:** kegagalan jaringan/parsing tidak menghentikan pipeline; ada retry & dead-letter.
- **Observability:** logging terstruktur + metrik + tracing minimal sejak awal.
- **Maintainability:** per-source effort rendah; core stabil; separation of concerns tegas.
- **Data quality:** SLA kualitas terukur (mis. ≥95% record punya tanggal valid; ≥90% punya lokasi).
- **Cost-awareness:** hemat bandwidth & storage (dedup, kompresi, columnar).

## 1.5 Asumsi

- Awalnya dijalankan single researcher / mesin tunggal, mode batch (bukan real-time).
- Konten berbahasa Indonesia; struktur HTML tiap portal berbeda dan **bisa berubah sewaktu-waktu**.
- Mayoritas portal **tidak** menyediakan API resmi; discovery via halaman pencarian/indeks/sitemap/RSS.
- Sebagian besar artikel dapat di-render server-side (HTML statis); sebagian kecil butuh JS (ditangani terpisah).
- Penggunaan untuk riset akademik (mempengaruhi argumen fair use, §13).

## 1.6 Constraint

- **Legal/ToS:** beberapa portal melarang scraping; harus ada kebijakan patuh + allowlist/denylist per sumber.
- **HTML volatility:** selector akan sering rusak → butuh strategi config + monitoring parse yield.
- **Resource terbatas:** budget compute/storage skripsi terbatas → prioritaskan efisiensi & local-first tooling.
- **Bahasa & lokalitas:** tanggal berbahasa Indonesia ("12 Mei 2026"), nama daerah non-standar → butuh normalisasi khusus.
- **Copyright:** teks berita berhak cipta → batasi redistribusi (simpan URL; pertimbangkan derived features).

---

# 2. High Level Architecture

Diagram linear yang kamu buat benar secara _alur data_, tetapi secara arsitektur perlu di-_layer_ menjadi empat lapis dengan **boundary tegas antara Acquisition dan Processing**. Alasannya: HTML akan berubah, aturan parsing akan berkembang, dan kamu akan sering perlu mem-_parse ulang_ data lama. Kalau parsing menempel pada fetch, setiap perbaikan parser memaksa crawl ulang — mahal, lambat, dan memperbesar beban ke server sumber (masalah etika).

## 2.1 Empat Lapis

```
LAYER 1 — ACQUISITION (crawl-time, menyentuh internet)
  Keyword → Search/Discovery → URL Frontier → Fetcher → RAW STORE (Bronze)
                                                        ├─ raw HTML (immutable)
                                                        └─ raw image bytes (immutable)

LAYER 2 — PROCESSING (batch-time, offline, tidak menyentuh internet)
  RAW STORE → Parser → Field Extraction → Image Processing → Normalization → SILVER STORE

LAYER 3 — QUALITY
  SILVER → Validation → Deduplication → Quality Gates → (quarantine | pass)

LAYER 4 — SERVING / DATASET
  Passed records → ETL/Assembly → GOLD STORE (Parquet/JSON) → Catalog → Dataset Final
```

Perhatikan: **Bronze store adalah checkpoint kritis**. Semua stage setelahnya bersifat _pure function_ atas Bronze — bisa diulang, di-backfill, dan diaudit tanpa menyentuh internet lagi.

## 2.2 Tanggung Jawab Tiap Komponen (memetakan diagram-mu)

| Komponen (diagram-mu)           | Layer | Tanggung jawab                                                                                                | Catatan desain                                        |
| ------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| **Keyword**                     | 1     | Sumber intent; mendefinisikan query pencarian per sumber.                                                     | Simpan sebagai config, bukan hardcode.                |
| **Crawler / Article Discovery** | 1     | Menemukan URL artikel relevan (via search page, sitemap, RSS, indeks kategori).                               | Output: kandidat URL + konteks (keyword, sumber).     |
| **Article Fetcher**             | 1     | Mengambil HTML mentah; menerapkan politeness, retry, cache.                                                   | **Hanya fetch + simpan.** Tidak parsing.              |
| **Parser**                      | 2     | Mengubah raw HTML → field terstruktur via adapter per-source.                                                 | Idempotent; input dari Bronze.                        |
| **Image Downloader**            | 1     | Mengunduh bytes gambar; simpan raw immutable (content-addressed).                                             | Acquisition, bukan processing.                        |
| **Metadata Extraction**         | 2     | Menarik tanggal, penulis, lokasi, jenis/dampak bencana, plus metadata teknis (JSON-LD, OpenGraph, meta tags). | JSON-LD/OG sering lebih stabil daripada scraping DOM. |
| **Validation**                  | 3     | Cek schema + aturan kualitas; tandai lolos/gagal.                                                             | Gate, bukan mutator.                                  |
| **Cleaning**                    | 2/3   | Buang boilerplate, iklan, whitespace, entitas HTML.                                                           | Sebelum normalization.                                |
| **Deduplication**               | 3     | Hilangkan duplikat URL/artikel/gambar.                                                                        | Content hash + near-dup (§9, §10).                    |
| **Normalization**               | 2     | Standarkan tanggal/lokasi/encoding/skema.                                                                     | Deterministik & versioned.                            |
| **ETL**                         | 4     | Rakit record final, join entitas, tulis kolom-kolom dataset.                                                  | Batch, columnar.                                      |
| **Dataset Storage / Final**     | 4     | Parquet/JSON + katalog + partisi.                                                                             | Immutable per-release/versi.                          |

## 2.3 Mengapa layering ini > pipeline linear

- **Re-parseability:** perbaiki bug parser tanpa re-crawl.
- **Backfill:** tambah field baru (mis. ekstraksi dampak) untuk seluruh histori dengan menjalankan ulang Layer 2 saja.
- **Reproducibility & audit:** Bronze menjadi _source of truth_ yang immutable; Gold selalu bisa direkonstruksi.
- **Kebijakan crawl lebih ringan ke server sumber:** kamu fetch sekali, olah berkali-kali → lebih sopan (langsung terhubung ke §13).

---

# 3. Scraping Engine Architecture

Bagian ini membedah Layer 1 (dan sebagian Layer 2) menjadi komponen operasional. Untuk tiap komponen: **[WAJIB awal]**, **[Bangun awal, versi sederhana]**, atau **[Opsional/nanti]**.

## 3.1 Search Manager — **[WAJIB awal]**

Menerjemahkan `(keyword × sumber)` menjadi _seed URLs_ discovery. Untuk tiap sumber, ia tahu cara membentuk query: URL halaman pencarian, endpoint pencarian internal, sitemap/RSS, atau indeks kategori "bencana". Output: daftar seed + konteks (keyword, source_id, discovery_method).
Trade-off: search page tiap portal rapuh & berbeda; **sitemap.xml dan RSS jauh lebih stabil** dan sering memuat tanggal terbit — prioritaskan itu bila tersedia.

## 3.2 Scheduler — **[Bangun awal, versi sederhana]**

Mengatur _kapan_ pekerjaan dijalankan (discovery harian, re-crawl berkala, backfill). Awal cukup cron/manual trigger. Nanti berkembang ke orchestrator (§11). Jangan over-engineer di awal.

## 3.3 Crawl Queue / URL Frontier — **[WAJIB awal]**

Jantung crawler. Menyimpan URL yang menunggu diproses, dengan properti:

- **Dedup URL** (canonicalized) agar tidak fetch dua kali.
- **Politeness per-host:** antrian dipartisi per domain; tiap domain punya _ready time_ sendiri agar rate limit tidak saling mengganggu (konsep dari desain crawler skala besar seperti Mercator).
- **Prioritas** (opsional): artikel baru > lama.
- **Persisted & resumable:** frontier disimpan (DB), bukan hanya in-memory, agar crash tidak menghapus progres.
  Trade-off in-memory vs persisted: in-memory cepat & simpel tapi hilang saat crash. Untuk skala target, **frontier persisted wajib**.

## 3.4 Worker / Fetcher — **[WAJIB awal]**

Mengeksekusi HTTP GET, menerapkan header, timeout, dan menyerahkan hasil ke Raw Store. **Concurrency dibatasi per-domain**, bukan global, agar sopan. Awal: async single-machine (asyncio/httpx atau Scrapy). Skala: banyak worker terdistribusi (§11).

## 3.5 Source Adapter — **[WAJIB awal, ini investasi terpenting]**

Abstraksi per-sumber: cara discovery + cara parsing. **Kontrak adapter yang stabil adalah keputusan desain paling menentukan** karena menjaga core tetap sumber-agnostik (§4). Bangun interface-nya dengan benar sejak awal walau implementasinya baru satu.

## 3.6 Parser — **[WAJIB awal]**

Mengubah raw HTML → field. Kombinasikan tiga strategi berlapis (fallback):

1. **Structured metadata dulu:** JSON-LD (`schema.org/NewsArticle`), OpenGraph, meta tags → paling stabil, sering memuat judul, tanggal, penulis, gambar utama.
2. **Config selector (CSS/XPath)** per sumber untuk isi/lokasi.
3. **Generic content extractor** (mis. algoritma readability/boilerplate removal) sebagai jaring pengaman.
   Trade-off: JSON-LD tidak selalu ada/akurat; selector rapuh; generic extractor "kotor". Berlapis = robust.

## 3.7 Image Downloader — **[WAJIB awal]**

Lihat §9. Intinya: acquisition-only, content-addressed, sopan (rate limit CDN gambar juga).

## 3.8 Rate Limiter — **[WAJIB awal]**

Per-domain token bucket + jitter + `Crawl-delay` dari robots. Ini bukan fitur opsional; ini kewajiban etis & pertahanan anti-blocking (§13).

## 3.9 Retry Engine — **[WAJIB awal]**

Exponential backoff + jitter untuk error transient (5xx, timeout, connection reset). Bedakan **retryable** (5xx, timeout) vs **non-retryable** (404, 403 permanen). Batas retry → kirim ke **dead-letter queue** untuk inspeksi, bukan buang diam-diam.

## 3.10 Cache — **[Bangun awal, versi sederhana]**

HTTP cache (respect `ETag`/`Last-Modified`) + "seen URL set". Karena kita sudah menyimpan raw ke Bronze, Bronze _sendiri_ berfungsi sebagai cache acquisition: sebelum fetch, cek apakah URL sudah ada di Bronze dan masih fresh. Hemat bandwidth + sopan.

## 3.11 Logging — **[WAJIB awal]**

Structured logging (JSON) dengan korelasi: `run_id`, `source_id`, `url`, `stage`, `status`, `latency`, `error_type`. Ini fondasi debugging & audit; retrofit belakangan menyakitkan.

## 3.12 Monitoring — **[Opsional awal → wajib saat skala]**

Metrik: crawl rate, success/error ratio per sumber, **parse yield** (rasio field berhasil diekstrak per sumber — ini early warning saat HTML sumber berubah), dedup rate, queue depth. Awal cukup log agregat; nanti dashboard (§11, §14).

## 3.13 Error Handling — **[WAJIB awal]**

Kebijakan menyeluruh: klasifikasi error (network, HTTP, parse, validation), keputusan retry/skip/quarantine, dan dead-letter. Prinsip: **fail-soft per item, fail-loud per sistem** — satu artikel gagal tidak menjatuhkan run; anomali sistemik (mis. semua parse gagal untuk satu sumber) memicu alert.

## 3.14 Ringkasan prioritas

| Komponen                                                                                                                                            | Prioritas                |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| Source Adapter (kontrak), Frontier persisted, Fetcher, Parser berlapis, Rate Limiter, Retry, Raw Store, Structured Logging, Error policy, URL dedup | **WAJIB SEJAK AWAL**     |
| Scheduler sederhana, Cache (via Bronze), Monitoring agregat                                                                                         | **Bangun awal, minimal** |
| Distributed queue, dashboard, CAPTCHA handling, headless browser pool                                                                               | **Nanti / opsional**     |

---

# 4. Multi Source Design

Masalah inti: tiap portal punya HTML berbeda, dan kamu ingin **menambah sumber tanpa menyentuh core engine** (Open/Closed Principle). Mari bandingkan pendekatan.

## 4.1 Perbandingan pola

| Pola                    | Ide                                                                                                                | Kelebihan                                                   | Kekurangan                                             | Cocok?                       |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------ | ---------------------------- |
| **Strategy Pattern**    | Tiap sumber = strategi parsing/discovery yang bisa ditukar di runtime.                                             | Bersih, testable, isolasi per-sumber.                       | Masih butuh 1 kelas per sumber.                        | ✅ Inti                      |
| **Factory**             | Objek pusat membuat adapter yang tepat dari `source_id`.                                                           | Decoupling instansiasi; core tak tahu detail.               | Hanya menyelesaikan pembuatan, bukan struktur parsing. | ✅ Pendamping                |
| **Plugin Architecture** | Adapter didaftarkan via registry/entry-point; core menemukannya otomatis.                                          | Tambah sumber = tambah file/plugin, **nol perubahan core**. | Sedikit overhead infrastruktur registry.               | ✅ Untuk skala 50–100 sumber |
| **Template Method**     | Skeleton crawl (fetch→store→parse→emit) di base class; sumber hanya override langkah spesifik (selector, tanggal). | Menghilangkan duplikasi alur; konsistensi.                  | Bisa kaku bila alur antar-sumber sangat beragam.       | ✅ Untuk kerangka umum       |

## 4.2 Rekomendasi: **kombinasi berlapis + config-driven**

Bukan memilih satu, tapi mengomposisikan:

1. **Template Method** untuk kerangka umum: langkah `discover → fetch → store_raw → extract → normalize → emit` sama untuk semua; base class mengurus politeness, retry, logging.
2. **Strategy** untuk bagian yang beda: `discovery strategy` dan `extraction strategy` per sumber.
3. **Factory + Plugin Registry** agar core memuat adapter dari registry berdasarkan `source_id` — tambah sumber = daftarkan adapter baru, core tak berubah.
4. **Config-driven extraction (kunci efisiensi):** untuk sumber "mudah" (HTML statis, selector stabil), adapter cukup **file konfigurasi deklaratif** (YAML) berisi selector CSS/XPath + aturan tanggal/lokasi. Hanya sumber "sulit" (JS berat, struktur aneh) yang butuh **kode custom**. Ini menekan per-source effort dari "tulis kelas" menjadi "tulis 15 baris YAML".

**Analoginya:** ini pola yang dipakai Scrapy (satu Spider per situs, item pipeline bersama) digabung dengan pendekatan _site-config_ yang umum di scraper berita skala besar. Untuk 100 sumber, mayoritas jadi entri config; hanya belasan yang butuh kode.

**Trade-off yang diakui:** config-driven menambah satu lapis "mesin interpreter selector" yang harus kamu rawat, dan kasus edge (pagination aneh, infinite scroll) tetap memaksa kode. Tetapi ROI-nya besar begitu sumber >10.

## 4.3 Kontrak adapter (konseptual, bukan kode)

Setiap adapter memenuhi kontrak minimum:

- `source_id`, metadata sumber (base URL, robots policy, rate limit).
- `discover(keyword) → iterable<candidate_url + context>`
- `parse(raw_html, url) → ArticleRecord (partial)`
- `locate_images(raw_html) → list<image_url>`
  Core hanya bicara ke kontrak ini. Selama kontrak stabil, sumber datang-pergi tanpa menyentuh core.

---

# 5. ETL Pipeline

ETL di sini mengadopsi **medallion architecture** (Bronze/Silver/Gold) — pola lakehouse yang matang dan memetakan langsung ke diagram HTML→…→Dataset milikmu.

## 5.1 Extraction (E) — menghasilkan **Bronze**

- Fetch raw HTML → simpan **immutable** (content-addressed: `sha256(html)`), plus manifest metadata acquisition (`url`, `fetched_at`, `http_status`, `source_id`, `keyword`, `content_type`, `etag`).
- Fetch raw image bytes → simpan **immutable** (content-addressed: `sha256(bytes)`), plus manifest (`origin_url`, `referrer_article`, `mime`, `bytes_len`).
- **Prinsip:** Bronze tidak pernah diubah. Ini kontrak reproducibility.

## 5.2 Transformation (T) — menghasilkan **Silver**

Urutan sesuai diagram-mu, diperjelas:

```
Raw HTML (Bronze)
  → Clean HTML        (buang script/style/nav/iklan/boilerplate)
  → Extract Field     (JSON-LD/OG → selector → generic fallback)
  → Normalize         (tanggal → ISO-8601+TZ; lokasi → terstandar; encoding; whitespace)
  → Image Processing  (decode-verify, hash, resize/thumbnail, EXIF, corrupt detection)
  → Hashing           (content hash artikel: simhash/minhash untuk near-dup)
  → Emit Silver record (schema-conform, satu artikel = satu baris + relasi gambar)
```

Silver = data bersih, ternormalisasi, **belum tentu unik & belum tentu lolos quality gate**.

## 5.3 Quality gate (antara Silver → Gold)

```
Silver
  → Duplicate Detection (URL exact, artikel near-dup, image exact/near)
  → Validation          (schema + rule: tanggal valid? field wajib ada? panjang minimum? lokasi non-kosong?)
  → Route: PASS → kandidat Gold ; FAIL → quarantine (dengan alasan)
```

## 5.4 Loading (L) — menghasilkan **Gold / Dataset Final**

- Rakit record final (join artikel ↔ gambar ↔ lokasi ↔ disaster info).
- Tulis ke **Parquet** (partisi by `source` dan `publish_date` bulanan) sebagai format utama analitik/training.
- Ekspor turunan: **JSON/JSONL** (portabilitas & konsumsi ML), opsional **HuggingFace datasets** format.
- Daftarkan ke **catalog** (tabel metadata: versi dataset, jumlah record, checksum, rentang tanggal, distribusi kelas).

## 5.5 Sifat wajib ETL

- **Idempotent:** re-run stage = hasil sama, tanpa duplikasi (content-addressing menjamin ini).
- **Incremental:** hanya proses raw baru/berubah (berdasarkan manifest & watermark waktu).
- **Versioned:** Gold dirilis sebagai versi immutable (`v1`, `v2`) → eksperimen ML reproducible.
- **Backfillable:** ubah logika transform → jalankan ulang Layer 2–4 atas Bronze penuh.

---

# 6. Data Model

Gunakan model **ternormalisasi & extensible**: pisahkan entitas agar penambahan field/relasi tidak merusak yang lama. ID berbasis konten agar stabil & dedup-friendly.

## 6.1 Entitas

**Article**

- `article_id` (PK) = `sha256(canonical_url)` atau `sha256(normalized_title + publish_date)` — stabil & idempotent.
- `source_id` (FK), `url` (canonical), `title`, `summary`, `body_clean`, `body_hash` (simhash), `author`, `publish_date` (ISO-8601 + TZ), `crawl_context` (keyword pemicu), `language`.
- `raw_html_ref` (pointer ke Bronze / hash).

**Image**

- `image_id` (PK) = `sha256(image_bytes)` → dedup exact gratis.
- `article_id` (FK), `role` (main/content), `origin_url`, `phash` (near-dup), `width`, `height`, `format`, `bytes_len`, `is_valid`, `storage_ref`.
- Relasi Article↔Image: **many-to-many** (satu gambar bisa dipakai ulang lintas artikel — sering terjadi di jaringan media).

**Source**

- `source_id` (PK), `name`, `base_url`, `adapter_type` (config|custom), `robots_policy`, `rate_limit`, `trust_tier`.

**Location**

- `location_id`, `raw_text`, `admin_level` (provinsi/kab-kota/kecamatan), `standard_name`, `lat`, `lon`, `geocode_confidence`, `geocode_source`.
- Dipisah karena satu artikel bisa punya banyak lokasi, dan geocoding adalah proses berbeda (bisa di-backfill).

**DisasterInfo**

- `disaster_type` (banjir/longsor/gempa/…; enum extensible), `severity_label` (nullable — diisi downstream oleh anotasi/model), `impact` (korban, rumah rusak, pengungsi — struktur key-value), `event_date`, `extraction_confidence`.
- Pisahkan `severity_label` dari artikel: label adalah **derived/annotation**, bukan fakta mentah crawl.

**CrawlMetadata / Provenance**

- `fetched_at`, `run_id`, `http_status`, `parser_version`, `schema_version`, `discovery_method`.
- **Provenance wajib** demi audit & reproducibility.

## 6.2 Prinsip extensibility

- **Schema versioning** (`schema_version` di tiap record) → evolusi tanpa breaking.
- **Enum terbuka** untuk `disaster_type` (tambah "abrasi", "kekeringan" tanpa migrasi besar).
- **Additive changes only** di Gold: tambah kolom, jangan ubah makna kolom lama.
- **Nullable derived fields**: `severity_label`, `lat/lon` boleh kosong di v1, diisi backfill.

---

# 7. Folder Structure

Dirancang untuk skala besar (100 situs, jutaan artikel, puluhan juta gambar). Prinsip: **pisahkan kode dari data, pisahkan tiap layer medallion, dan shard direktori gambar**.

```
disaster-scraper/
├── README.md
├── pyproject.toml                 # dependency & tooling
├── config/
│   ├── sources/                   # satu file per sumber (config-driven adapters)
│   │   ├── antara.yaml
│   │   ├── kompas.yaml
│   │   └── ...
│   ├── keywords.yaml
│   ├── crawl_policy.yaml           # rate limit, robots, UA, concurrency
│   └── settings.yaml               # env: paths, storage backend, dsb.
│
├── src/
│   ├── core/                       # ENGINE — jarang berubah
│   │   ├── frontier/               # URL frontier persisted + politeness
│   │   ├── fetcher/                # HTTP, rate limiter, retry, cache
│   │   ├── adapter/                # base adapter, registry, template method
│   │   ├── parser/                 # JSON-LD/OG/selector/generic extractor
│   │   ├── storage/                # abstraksi Bronze/Silver/Gold backend
│   │   ├── quality/                # validation, dedup, quality gates
│   │   ├── imaging/                # image pipeline
│   │   └── observability/          # logging, metrics
│   ├── adapters/
│   │   ├── config_driven/          # loader untuk sumber berbasis YAML
│   │   └── custom/                 # kode khusus sumber sulit
│   ├── pipelines/                  # orkestrasi tiap stage (discovery, fetch, parse, etl)
│   └── cli/                        # entrypoint perintah
│
├── data/                          # BUKAN di git — di object storage / disk terpisah
│   ├── bronze/
│   │   ├── html/{source}/{yyyy}/{mm}/{sha256}.html.gz
│   │   └── images_raw/{ab}/{cd}/{sha256}.bin       # shard by hash prefix
│   ├── silver/{source}/{yyyy-mm}/*.parquet
│   ├── gold/
│   │   ├── articles/v1/{partitioned parquet}
│   │   └── images/{ab}/{cd}/{sha256}.jpg           # processed, sharded
│   ├── quarantine/                 # record gagal + alasan
│   └── catalog/                    # metadata dataset & manifests
│
├── ops/
│   ├── docker/
│   ├── orchestration/              # Airflow/Prefect DAGs (fase skala)
│   └── monitoring/
├── tests/
│   ├── unit/
│   ├── adapters/                   # test parsing per sumber pakai HTML fixture
│   └── fixtures/                   # snapshot HTML nyata untuk regression parsing
└── docs/
    └── TDD.md                      # dokumen ini
```

**Detail yang menyelamatkanmu di skala besar:**

- **Sharding direktori gambar** (`{sha256[:2]}/{sha256[2:4]}/…`): menaruh jutaan file dalam satu folder membunuh filesystem (inode, `ls`, backup). Shard 2-level = ~65k subfolder, distribusi merata.
- **Partisi Silver/Gold by source + bulan**: query & backfill selektif, partition pruning di DuckDB/Spark.
- **`tests/fixtures` berisi snapshot HTML nyata**: begitu HTML sumber berubah & parse yield turun, test regression menangkapnya lebih dulu.
- **`data/` di luar git**, di object storage atau disk khusus, dengan lifecycle policy (§8).

---

# 8. Storage Design

Kesalahan umum: memakai satu format untuk semua stage. Yang benar: **format berbeda untuk concern berbeda** (transaksional vs analitik vs blob).

## 8.1 Perbandingan opsi

| Format/Store                  | Kekuatan                                                               | Kelemahan                                                                       | Peran tepat di pipeline                                                                  |
| ----------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| **JSON/JSONL**                | Human-readable, portabel, ML-friendly.                                 | Boros ruang, lambat untuk query analitik, tak ada tipe/kompresi kolumnar.       | Ekspor turunan Gold; manifest kecil.                                                     |
| **CSV**                       | Universal, sederhana.                                                  | Tak ada tipe, rusak dengan koma/newline dalam teks (isi berita!), tak scalable. | **Hindari** untuk isi berita; hanya laporan ringkas.                                     |
| **SQLite**                    | Zero-server, transaksional, cukup untuk crawl-state kecil.             | Konkuren-tulis terbatas; tak untuk analitik besar.                              | State crawl/frontier di fase skripsi tunggal.                                            |
| **PostgreSQL**                | Transaksional kuat, konkuren, index, JSONB.                            | Perlu server; bukan untuk scan analitik kolom besar.                            | **OLTP: frontier, dedup index, provenance, katalog** saat skala naik.                    |
| **MongoDB**                   | Skema fleksibel, dokumen nested.                                       | Konsistensi/analitik lemah; mudah jadi "data swamp".                            | Opsional untuk staging semi-terstruktur; **bukan wajib**. Postgres+JSONB biasanya cukup. |
| **Parquet**                   | Kolumnar, terkompresi, cepat untuk analitik/training, schema-embedded. | Bukan untuk update baris/transaksional.                                         | **Silver & Gold — format utama dataset.**                                                |
| **DuckDB**                    | OLAP in-process, query Parquet langsung, nol infra.                    | Bukan multi-writer server.                                                      | **Analitik lokal, quality checks, eksplorasi dataset.** Ideal untuk skripsi.             |
| **Object Storage (S3/MinIO)** | Skalabel tak terbatas, murah, immutable-friendly.                      | Latensi objek, bukan untuk query relasional.                                    | **Bronze (raw HTML/images) & Gold besar** di fase skala. Lokal: MinIO atau filesystem.   |

## 8.2 Pemetaan per stage (rekomendasi)

- **Bronze (raw HTML, raw images):** filesystem lokal (skripsi) → object storage (MinIO/S3) saat skala. Immutable, content-addressed, gzip untuk HTML.
- **Crawl state (frontier, seen-set, dedup index, provenance):** SQLite (single-node) → PostgreSQL (multi-worker).
- **Silver & Gold (dataset):** **Parquet** (partitioned), di-query dengan **DuckDB**.
- **Catalog/metadata dataset:** PostgreSQL (atau tabel Parquet manifest bila ingin nol-server).
- **Images processed:** file sharded (§7) lokal → object storage.

**Prinsip pemandu:** _transaksional → Postgres/SQLite; analitik → Parquet+DuckDB; blob → object storage/filesystem._ Jangan paksa satu tool lintas peran.

---

# 9. Image Pipeline

Gambar adalah warga kelas satu (ini dataset _multimodal_). Perlakukan dengan disiplin setara teks.

## 9.1 Alur

```
Image URL (dari parser)
  → Download bytes (politeness ke CDN gambar; retry)
  → Store RAW immutable: sha256(bytes) → images_raw/{shard}/{sha256}.bin
  → Decode-verify (Pillow: bisa di-decode? bukan HTML error page menyamar jadi gambar?)
  → Exact hash = sha256(bytes)      → dedup persis
  → Perceptual hash (pHash)         → dedup near-duplicate (crop/resize/rekompresi)
  → Metadata extract (dimensi, format, mode warna, EXIF bila ada)
  → Corrupt/anomaly detection (truncated, 0-byte, mime mismatch, ukuran ekstrem)
  → Resize / thumbnail (mis. simpan original + versi maks 1024px + thumbnail 256px)
  → Emit Image record (link ke article_id, is_valid, hashes, refs)
```

## 9.2 Keputusan penting

- **Content-addressed naming (`sha256`)**: nama file = hash isi → dedup exact otomatis, idempotent (download ulang menimpa identik), tak ada tabrakan nama.
- **Exact (sha256) + perceptual (pHash) hashing**: sha256 menangkap byte-identik; pHash menangkap "gambar sama di-resize/rekompres" yang umum di sindikasi berita. Simpan keduanya.
- **Decode-verify wajib**: portal sering mengembalikan halaman error/placeholder ber-`Content-Type: image`. Verifikasi decode mencegah "gambar palsu" masuk dataset.
- **Simpan original + turunan**: original untuk reproducibility; turunan (≤1024px + thumbnail) untuk training & preview. Jangan buang original terlalu dini.
- **Folder sharding** (§7): mutlak untuk puluhan juta file.
- **Rate limit CDN gambar** terpisah dari rate limit halaman artikel — sering host berbeda.
- **Batas ukuran & MIME allowlist**: tolak file >N MB atau format tak dikenal (pertahanan bandwidth & keamanan).

## 9.3 Trade-off

pHash menambah komputasi & risiko false-positive (dua foto banjir berbeda bisa mirip). Solusi: pakai pHash sebagai _kandidat_ near-dup, lalu ambang jarak Hamming konservatif + tinjau manual sampel. Jangan auto-hapus agresif di dataset riset — **arantina, jangan musnahkan**.

---

# 10. Data Quality Pipeline

Kualitas data menentukan validitas hasil ML-mu. Perlakukan sebagai **quality gates** eksplisit dengan aturan versioned, bukan pembersihan ad-hoc. Pola industri: framework ekspektasi data (à la Great Expectations / Pandera) — cek terdeklarasi, hasil terukur, kegagalan terarantina.

## 10.1 Deteksi yang wajib

| Masalah                       | Metode deteksi                                                           | Aksi                                                                                             |
| ----------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| **Duplicate article (exact)** | Canonical URL + content sha256.                                          | Drop duplikat, simpan satu + daftar alias URL.                                                   |
| **Duplicate article (near)**  | SimHash/MinHash+LSH atas `body_clean` (menangkap re-publish/sindikasi).  | Klaster near-dup; pilih kanonik (sumber trust tertinggi / terbit terawal).                       |
| **Duplicate image**           | sha256 (exact) + pHash (near).                                           | Dedup; pertahankan relasi many-to-many (§6).                                                     |
| **Broken image**              | Decode-verify gagal / 0-byte / mime mismatch.                            | Tandai `is_valid=false`, arantina.                                                               |
| **Missing field**             | Aturan field wajib (judul, isi, tanggal).                                | Arantina + alasan; kandidat re-parse.                                                            |
| **Tanggal tidak valid**       | Parser tanggal ID gagal / di luar rentang wajar (future date, pra-2000). | Arantina; coba fallback (OG/JSON-LD/URL slug).                                                   |
| **Lokasi kosong**             | Field lokasi null setelah ekstraksi + NER ringan.                        | Tandai; boleh masuk Gold dengan flag `location_missing` (jangan buang; downstream bisa geocode). |
| **Artikel terlalu pendek**    | `len(body) < threshold` (mis. <300 karакter).                            | Arantina (kemungkinan teaser/paywall/parse gagal).                                               |
| **Noise / boilerplate**       | Rasio boilerplate tinggi, banyak "Baca juga", menu nav bocor.            | Cleaning ulang; bila gagal, arantina.                                                            |
| **Spam / non-berita**         | Heuristik (judul clickbait tanpa isi, listicle, iklan).                  | Filter; arantina untuk review.                                                                   |
| **Off-topic (bukan bencana)** | Keyword relevansi + klasifikasi ringan (opsional).                       | Flag `low_relevance`; jangan buang otomatis di riset.                                            |

## 10.2 Prinsip

- **Quarantine, jangan delete.** Data riset mahal; record gagal sering bisa diselamatkan dengan re-parse. Simpan alasan kegagalan agar bisa dianalisis (mis. "80% quarantine dari Tribun → selector rusak").
- **Quality metrics sebagai SLA**: pantau % record lolos per aturan per sumber; penurunan mendadak = sinyal HTML berubah.
- **Aturan versioned**: `quality_ruleset_version` menempel di Gold agar reproducible.
- **Gate berlapis**: hard rules (schema, field wajib) = blocking; soft rules (relevansi, panjang) = flag, bukan blokir.

---

# 11. Scalability

Skenario target: 50 situs, 100 keyword, 5 juta artikel, puluhan juta gambar. Kunci: **desain awal sudah "scale-ready" sehingga naik skala = ganti implementasi komponen, bukan rewrite arsitektur.**

## 11.1 Yang **tetap** (karena sudah benar sejak awal)

- Boundary Acquisition↔Processing dan medallion Bronze/Silver/Gold.
- Kontrak Source Adapter (config-driven).
- Content-addressed IDs & idempotency.
- Schema data & provenance.
- Quality gates & catalog.

Ini alasan kenapa kita "membangun benar sejak kecil": semua di atas _invariant_ terhadap skala.

## 11.2 Yang **berubah** saat naik skala

| Komponen         | Skala kecil (skripsi)                | Skala besar                                                 |
| ---------------- | ------------------------------------ | ----------------------------------------------------------- |
| Frontier/Queue   | SQLite / in-process async            | Redis / RabbitMQ / Kafka (distributed queue)                |
| Worker           | asyncio single-machine (atau Scrapy) | Banyak worker terdistribusi (Scrapy-Redis / container pool) |
| Crawl-state DB   | SQLite                               | PostgreSQL (managed)                                        |
| Raw & Gold store | Filesystem lokal                     | Object storage (S3/MinIO) + partitioned Parquet             |
| Orkestrasi       | Cron / CLI manual                    | Airflow / Prefect / Dagster (DAG, retry, backfill)          |
| Analitik         | DuckDB lokal                         | DuckDB/Spark atas Parquet di object storage                 |
| Monitoring       | Log agregat                          | Prometheus + Grafana; alert parse-yield                     |
| Dedup near-dup   | In-memory LSH                        | LSH terdistribusi / service khusus                          |

## 11.3 Pola scaling yang diperlukan

- **Horizontal scaling worker** dengan queue terdistribusi (producer discovery → consumer fetcher).
- **Backpressure**: batasi kedalaman queue & concurrency per-domain agar tidak membanjiri sumber (sekaligus etika, §13).
- **Partitioning**: by source + date → paralelisme ETL & partition pruning.
- **Incremental & watermarking**: hanya proses delta; hindari full re-scan tiap run.
- **Decoupling via message broker**: discovery, fetch, parse, image jadi service independen yang bisa diskalakan terpisah sesuai bottleneck.

**Prinsip:** mulai monolith-modular (satu proses, modul terpisah bersih), pecah jadi service hanya saat bottleneck nyata muncul — jangan prematurely distribute (biaya kompleksitas > manfaat di fase skripsi).

---

# 12. Roadmap Implementasi

Diurutkan dari fondasi ke production-ready. Tiap sprint menghasilkan sesuatu yang _berjalan end-to-end_ (vertical slice), bukan lapisan horizontal yang belum tersambung.

**Sprint 0 — Design & Skeleton (fondasi)**

- Finalisasi TDD ini, schema data, dan kontrak Source Adapter.
- Siapkan struktur folder, tooling, config layout, storage abstraction (Bronze/Silver/Gold), logging.
- _Deliverable:_ kerangka proyek + schema versi 1 + kebijakan crawl (robots/rate limit).

**Sprint 1 — Vertical slice satu sumber (end-to-end)**

- Satu sumber "mudah" (mis. yang punya sitemap/RSS + JSON-LD). Discovery → fetch → Bronze → parse → Silver → validate → Gold (Parquet) untuk teks saja.
- _Deliverable:_ dataset mini teks dari 1 sumber, reproducible dari Bronze.

**Sprint 2 — Adapter framework + multi-source**

- Implementasi Template Method + Strategy + Registry + config-driven YAML loader.
- Tambah 2–3 sumber lewat config (buktikan "tambah sumber tanpa ubah core").
- _Deliverable:_ 3–4 sumber jalan; parse yield termonitor.

**Sprint 3 — Image pipeline**

- Download, content-addressing, decode-verify, hashing (sha256+pHash), resize/thumbnail, corrupt detection, sharding.
- _Deliverable:_ dataset **multimodal** (teks+gambar) untuk sumber yang ada.

**Sprint 4 — Deduplication & Data Quality gates**

- URL/near-dup artikel (SimHash/MinHash+LSH), dedup gambar, validation rules, quarantine + alasan, quality metrics.
- _Deliverable:_ Gold bersih + laporan kualitas per sumber.

**Sprint 5 — Normalization mendalam & Catalog**

- Normalisasi tanggal ID robust, standarisasi lokasi (persiapan geocoding), enum jenis bencana, katalog dataset + versioning rilis.
- _Deliverable:_ Dataset **v1** ter-tag, terdokumentasi, ter-katalog.

**Sprint 6 — Robustness & Observability**

- Retry/dead-letter matang, cache via Bronze, monitoring parse-yield + alert, regression tests parsing (HTML fixtures), incremental crawl.
- _Deliverable:_ pipeline yang tahan HTML berubah & bisa jalan berulang aman.

**Sprint 7+ — Scale-out (opsional, bila melampaui skripsi)**

- Distributed queue (Redis), object storage, orchestrator (Airflow/Prefect), managed Postgres, dashboard.
- _Deliverable:_ jalur production-grade.

**Aturan main roadmap:** jangan lompat ke Sprint 3+ sebelum Sprint 1 truly end-to-end. Satu sumber yang benar-benar tembus dari keyword sampai Gold mengajari kamu 80% masalah nyata sistem.

---

# 13. Risk Analysis

Diorganisir per risiko dengan mitigasi. Risiko **legal/etika ditempatkan sebagai kelas satu**, bukan catatan kaki.

| Risiko                           | Dampak                                              | Mitigasi                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| -------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **robots.txt / ToS**             | Melanggar aturan situs; risiko legal & pemblokiran. | Parse & patuhi `robots.txt` (termasuk `Crawl-delay`); simpan **kebijakan per-sumber** (allow/deny); jika ToS melarang scraping, jangan crawl sumber itu — dokumentasikan keputusan. Prioritaskan sumber yang menyediakan sitemap/RSS/feed resmi.                                                                                                                                                                                                                                                                   |
| **Rate limit / overload server** | Membebani server sumber; IP terblokir; tidak etis.  | Rate limiter per-domain + jitter + concurrency terbatas; hormati `Retry-After`; crawl di jam sepi; **fetch sekali, olah berkali-kali** (Bronze) untuk minimalkan hit.                                                                                                                                                                                                                                                                                                                                              |
| **Blocking / IP ban**            | Crawl terhenti.                                     | Backoff sopan, jangan agresif; UA jujur & kontak; **jangan** langsung lompat ke rotasi proxy/anti-bot evasion sebagai solusi pertama — itu menandakan kamu mengabaikan sinyal "berhenti" dari situs. Bila sumber jelas menolak, hentikan & cari sumber lain.                                                                                                                                                                                                                                                       |
| **CAPTCHA**                      | Konten tak terjangkau.                              | Perlakukan sebagai sinyal "jangan scrape di sini". **Jangan** memecahkan CAPTCHA — itu melanggar maksud situs dan berisiko etis/legal. Cari jalur resmi (RSS/API) atau drop sumber.                                                                                                                                                                                                                                                                                                                                |
| **HTML berubah**                 | Parser rusak, parse yield anjlok.                   | Ekstraksi berlapis (JSON-LD/OG dulu); regression tests dengan HTML fixtures; monitor parse-yield per sumber + alert; config-driven selector mudah diperbaiki tanpa deploy kode.                                                                                                                                                                                                                                                                                                                                    |
| **Website down / intermittent**  | Fetch gagal sebagian.                               | Retry+backoff, dead-letter, resumable frontier; jadwalkan ulang; jangan gagalkan seluruh run karena satu host.                                                                                                                                                                                                                                                                                                                                                                                                     |
| **Duplikasi (artikel/gambar)**   | Bias dataset, bocor train/test.                     | Content-addressing + near-dup (SimHash/pHash); klaster & pilih kanonik; **splitting harus sadar duplikat & sumber** agar tak bocor antar-split.                                                                                                                                                                                                                                                                                                                                                                    |
| **Kualitas data buruk**          | Model belajar dari noise.                           | Quality gates + quarantine + metrics + review sampel manual berkala.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **Storage membengkak**           | Biaya & pengelolaan.                                | Kompresi (gzip HTML, Parquet), dedup, simpan turunan gambar berukuran wajar, lifecycle policy, sharding.                                                                                                                                                                                                                                                                                                                                                                                                           |
| **Bandwidth**                    | Lambat & mahal.                                     | Cache via Bronze; hormati `ETag`/`Last-Modified`; jangan re-download; batasi ukuran gambar.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Legal & Copyright**            | Teks & foto berita **berhak cipta**.                | (a) Simpan **URL sumber & atribusi** di tiap record. (b) Gunakan untuk **riset** (argumen fair use/dealing lebih kuat untuk penelitian non-komersial, tapi bukan lisensi absolut). (c) **Jangan redistribusi teks penuh / foto** ke publik; bila dataset akan dibagikan, pertimbangkan **menyimpan derived features** (embedding, fitur, atau hanya URL+metadata) alih-alih teks/gambar mentah. (d) Untuk foto, hormati kredit fotografer/kantor berita. (e) Bila ragu, minta izin/konsultasi pembimbing & sumber. |
| **Privasi (PII)**                | Berita memuat nama korban, dsb.                     | Minimalkan & pertimbangkan redaksi PII bila dataset dibagikan; batasi pemakaian sesuai etika riset/institusi.                                                                                                                                                                                                                                                                                                                                                                                                      |
| **Reproducibility hilang**       | Hasil skripsi tak bisa diverifikasi.                | Bronze immutable + provenance + versioning dataset + kode ter-tag.                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

**Catatan sikap engineering yang benar (penting untuk skripsi & etika):** desain ini mengutamakan **crawling yang sopan dan patuh**, bukan "menang melawan anti-bot". Jika sebuah portal secara teknis/legal menolak diakses otomatis, jawaban yang tepat adalah mengurangi beban, mencari kanal resmi (RSS/API/sitemap), atau menggantinya — bukan meningkatkan agresivitas. Ini juga memperkuat validitas & pertanggungjawaban penelitianmu.

---

# 14. Tech Stack Recommendation

Untuk tiap pilihan: rekomendasi + alasan + trade-off. Konteks: single researcher, batch, local-first, tapi scale-ready.

## 14.1 HTTP client: `requests` vs `httpx` vs `aiohttp`

- **Rekomendasi: `httpx`** (bila membangun custom async) — API mirip `requests`, mendukung **async + HTTP/2**, cocok untuk konkurensi crawl.
- `requests`: paling sederhana tapi **sinkron** → tak efisien untuk ribuan halaman.
- `aiohttp`: async matang & cepat, tapi API lebih low-level. Pilihan valid bila kamu nyaman async murni.
- _Trade-off:_ jika pakai Scrapy (lihat 14.3), engine HTTP-nya sudah ditangani framework (berbasis Twisted async) — kamu tak perlu memilih ini secara manual.

## 14.2 HTML parser: `BeautifulSoup` vs `lxml` vs `selectolax`

- **Rekomendasi utama: `selectolax`** (backend Modest/lexbor) untuk kecepatan — signifikan lebih cepat pada volume besar, hemat CPU/memori.
- **`lxml`**: keseimbangan terbaik antara kecepatan & fitur (XPath penuh); pilihan aman.
- **`BeautifulSoup`**: paling ramah & toleran HTML rusak, tapi paling lambat → pakai untuk prototyping/kasus HTML sangat berantakan.
- _Trade-off:_ selectolax cepat tapi fitur XPath terbatas dibanding lxml. Strategi: **selectolax untuk mayoritas; lxml untuk kasus butuh XPath kompleks.**
- Untuk metadata terstruktur: baca **JSON-LD/OpenGraph** langsung (lebih stabil dari DOM scraping).

## 14.3 Framework: **Scrapy** vs custom async

- **Rekomendasi: Scrapy** sebagai fondasi (kecuali ada alasan kuat sebaliknya).
- Alasan: Scrapy **sudah menyediakan** hampir seluruh "Scraping Engine Architecture" di §3 — scheduler, URL frontier, dedup request, throttling (AutoThrottle), retry, robots.txt obedience, item pipeline, dan konkurensi async. Membangun ini dari nol = berbulan-bulan reinventing wheel yang sudah teruji.
- _Trade-off / kapan custom:_ jika kebutuhanmu sangat spesifik-async di luar model Scrapy, atau kamu ingin kontrol penuh untuk tujuan pembelajaran skripsi, custom (`httpx`+`asyncio`) sah — tapi sadari kamu menanggung sendiri politeness, retry, frontier, dsb. **Rekomendasi pragmatis:** pakai Scrapy untuk acquisition + frontier; letakkan logika parsing/adapter-mu (config-driven) di atasnya; jalankan ETL (Layer 2–4) sebagai proses terpisah atas Bronze. Untuk skala: **Scrapy-Redis** memberi distributed frontier hampir gratis (menyambung §11).

## 14.4 Headless browser: **Playwright** — _kapan_

- **Hanya untuk sumber yang benar-benar butuh JS rendering** (konten dimuat via XHR/JS, infinite scroll yang tak ada endpoint-nya).
- Mahal (CPU/memori, lambat) → **jangan jadikan default**. Deteksi dulu apakah HTML statis cukup; pakai Playwright sebagai _fallback per-source_, bukan untuk semua.
- Alternatif sebelum Playwright: cari **endpoint XHR/JSON** internal situs (sering lebih bersih & stabil daripada render DOM).

## 14.5 Concurrency: `asyncio` (+ engine di atasnya)

- Model async wajib untuk I/O-bound crawling. Bila pakai Scrapy, ini sudah tertangani. Bila custom, `asyncio` + `httpx` + semaphore per-domain.

## 14.6 State & queue: **Redis** / PostgreSQL

- **Redis**: distributed queue/frontier + seen-set saat scale-out (Scrapy-Redis). Di fase skripsi, opsional.
- **PostgreSQL**: crawl-state transaksional, dedup index, katalog saat multi-worker. SQLite cukup untuk single-node.

## 14.7 Analitik & dataset: **Parquet + DuckDB**

- **Parquet**: format Silver/Gold (kolumnar, terkompresi, schema-embedded, ML-friendly).
- **DuckDB**: OLAP in-process untuk query/validasi/eksplorasi dataset **tanpa server** — ideal untuk skripsi; bisa query Parquet langsung.

## 14.8 Image: **Pillow** (+ **OpenCV** bila perlu)

- **Pillow**: decode-verify, resize, thumbnail, EXIF, corrupt detection — cukup untuk mayoritas kebutuhan pipeline gambar.
- **OpenCV**: hanya bila butuh operasi CV lebih berat (pHash tertentu, deteksi konten, analisis piksel lanjutan). Untuk pHash sederhana, `imagehash` (di atas Pillow) sudah memadai.
- _Trade-off:_ OpenCV besar & berat dependensi; tambahkan hanya bila benar-benar dibutuhkan.

## 14.9 Ringkasan stack rekomendasi (fase skripsi → scale)

| Concern               | Skripsi (local-first)            | Scale-out                                |
| --------------------- | -------------------------------- | ---------------------------------------- |
| Acquisition/framework | **Scrapy** (+ config adapters)   | Scrapy-Redis                             |
| HTTP (bila custom)    | httpx + asyncio                  | idem, banyak worker                      |
| Parser                | selectolax + lxml + JSON-LD/OG   | idem                                     |
| JS rendering          | Playwright (fallback per-source) | pool Playwright                          |
| Crawl-state/queue     | SQLite                           | PostgreSQL + Redis                       |
| Raw store             | filesystem (gzip, sharded)       | Object storage (S3/MinIO)                |
| Dataset               | Parquet + DuckDB                 | Parquet di object storage + DuckDB/Spark |
| Image                 | Pillow (+imagehash)              | idem (+OpenCV bila perlu)                |
| Orkestrasi            | CLI/cron                         | Airflow/Prefect/Dagster                  |
| Observability         | structured logging               | Prometheus + Grafana                     |

---

## Penutup: benang merah antar-section

Empat keputusan ini mengikat seluruh dokumen — jika kamu hanya mengingat empat hal, ingat ini:

1. **Acquisition ≠ Extraction** (§2) → memungkinkan re-parse, backfill, reproducibility, dan crawling yang lebih sopan (§13).
2. **Medallion Bronze/Silver/Gold** (§5) → memberi struktur ETL, storage (§8), dan folder (§7) yang konsisten & scalable (§11).
3. **Config-driven adapters + kontrak stabil** (§4) → "tambah sumber tanpa ubah core" jadi kenyataan, per-source effort minimal.
4. **Content-addressing + idempotency** (§6, §9) → dedup natural, re-run aman, dataset reproducible.

Bangun **satu vertical slice end-to-end lebih dulu** (Sprint 1), lalu lebarkan. Arsitektur di atas dirancang agar melebar (lebih banyak sumber, lebih besar volume) tanpa memaksa rewrite — itulah perbedaan antara "scraper skripsi" dan "fondasi data pipeline yang bisa tumbuh production-grade".
