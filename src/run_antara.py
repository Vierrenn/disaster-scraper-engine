import sys
from pathlib import Path

from adapters.antara import parse
from fetch import bronze_path_for, fetch
from storage import save_record


def run(url: str) -> Path:
    html = fetch(url)
    bronze_file = bronze_path_for(html)

    # Baca ulang dari file Bronze (bukan variabel `html` di memori) 
    # menjaga prinsip: parse selalu terhadap hasil tersimpan, bukan fetch langsung.
    saved_html = bronze_file.read_text(encoding="utf-8")
    record = parse(saved_html)

    return save_record(record, bronze_file)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/run_antara.py <url>")
        sys.exit(1)

    result_path = run(sys.argv[1])
    print(f"Selesai. Hasil tersimpan di: {result_path}")
