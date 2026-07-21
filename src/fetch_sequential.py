"""Fetch banyak URL secara sequential (satu-satu) dari config/seed_urls.txt.
Baseline sebelum versi asyncio, hasil hanya masuk ke Bronze (data/bronze/html/ + manifest.json).
"""

import sys
import time
from pathlib import Path

from fetch import fetch

SEED_URLS_PATH = Path(__file__).resolve().parent.parent / "config" / "seed_urls.txt"


def load_urls(path: Path) -> list[str]:
    urls = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


def run(urls: list[str]) -> None:
    results = []
    start = time.perf_counter()

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] fetch: {url}")
        try:
            html = fetch(url)
            results.append((url, "OK", f"{len(html)} karakter"))
        except Exception as e:
            # 1 URL gagal tidak boleh menghentikan URL lainnya
            results.append((url, "GAGAL", f"{type(e).__name__}: {e}"))

    total_time = time.perf_counter() - start

    ok_count = sum(1 for _, status, _ in results if status == "OK")
    gagal_count = len(results) - ok_count

    print("\n=== RINGKASAN ===")
    print(f"Total URL   : {len(results)}")
    print(f"Berhasil    : {ok_count}")
    print(f"Gagal       : {gagal_count}")
    print(f"Total waktu : {total_time:.2f} detik")

    if gagal_count:
        print("\nDetail yang gagal:")
        for url, status, detail in results:
            if status == "GAGAL":
                print(f"  - {url}\n    {detail}")


if __name__ == "__main__":
    urls = load_urls(SEED_URLS_PATH)
    if not urls:
        print(f"Tidak ada URL ditemukan di {SEED_URLS_PATH}")
        sys.exit(1)
    run(urls)
