# disaster-scraper

Scraping engine untuk membangun dataset berita bencana multimodal (teks +
gambar) dari portal berita Indonesia, sebagai bagian dari penelitian skripsi
mengenai klasifikasi tingkat keparahan bencana.

## Latar belakang

Penelitian ini mengembangkan sistem klasifikasi tingkat keparahan bencana
menggunakan data multimodal (teks, gambar, metadata spasio-temporal).
Sebelum data bisa diolah dan dimodelkan, dibutuhkan dataset yang representatif
dari pemberitaan bencana di media Indonesia — itulah fungsi proyek ini.

Dataset yang dihasilkan akan digunakan untuk:

- Klasifikasi jenis bencana
- Klasifikasi tingkat keparahan
- Ekstraksi lokasi (NER)
- Analisis spasio-temporal
- Training model multimodal

## Sumber data

Portal berita yang menjadi target crawling meliputi Antara, Kompas, Detik,
Tempo, CNN Indonesia, Liputan6, Kumparan, Tribun, Radar Daerah, dan portal
BPBD daerah, dengan keyword seputar bencana (banjir, longsor, gempa, tsunami,
kebakaran hutan, dsb).

## Arsitektur singkat

Pipeline dibangun dengan prinsip memisahkan pengambilan data mentah
(_acquisition_) dari pengolahan data (_processing_), mengikuti pola tiga
tingkat penyimpanan:

- **Bronze** — HTML dan gambar mentah, tersimpan apa adanya, tidak pernah
  diubah.
- **Silver** — hasil parsing yang sudah terstruktur (judul, isi, tanggal,
  lokasi, dst), tapi belum divalidasi kualitasnya.
- **Gold** — dataset final yang sudah lolos pemeriksaan kualitas dan siap
  dipakai untuk pemodelan.

Desain lengkap beserta alasan di balik tiap keputusan arsitektur ada di
[`docs/TDD_Scraping_Engine_Bencana_Multimodal.md`](docs/TDD_Scraping_Engine_Bencana_Multimodal.md).

## Struktur folder

```
disaster-scraper/
├── docs/           # dokumen desain teknis (technical design document)
├── config/         # daftar keyword, konfigurasi sumber
├── src/            # kode program: fetcher, parser, pipeline
├── notebooks/      # eksplorasi struktur HTML tiap portal
├── tests/          # pengujian parser per sumber
└── data/
    ├── bronze/     # HTML & gambar mentah
    ├── silver/     # data hasil parsing
    ├── gold/       # dataset final
    └── quarantine/ # record yang gagal validasi, beserta alasannya
```

## Status

🔲 Tahap awal — pipeline untuk sumber pertama sedang dibangun.

## Roadmap

1. Satu sumber, teks saja, end-to-end
2. Menambah beberapa sumber lain
3. Pipeline gambar
4. Deduplikasi & quality gate
5. Normalisasi & katalog dataset
6. Robustness & observability

Detail tiap tahap ada di bagian roadmap implementasi pada dokumen desain.

## Etika & kepatuhan crawling

Crawling dilakukan dengan menghormati `robots.txt` tiap sumber, menerapkan
rate limiting yang wajar, dan tidak membebani server sumber secara
berlebihan. Data digunakan untuk kepentingan penelitian akademik.
