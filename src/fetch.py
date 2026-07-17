import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx

BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze" / "html"
MANIFEST_PATH = Path(__file__).resolve().parent.parent / "data" / "bronze" / "manifest.json"
RATE_LIMIT_SECONDS = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_last_request_time: dict[str, float] = {}


def _check_robots_allowed(url: str) -> bool:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        parser.read()
    except OSError:
        # robots.txt tidak bisa diakses -> anggap boleh (perilaku default umum)
        return True
    return parser.can_fetch(USER_AGENT, url)


def _respect_rate_limit(url: str) -> None:
    domain = urlparse(url).netloc
    now = time.monotonic()
    last = _last_request_time.get(domain)
    if last is not None:
        elapsed = now - last
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
    _last_request_time[domain] = time.monotonic()


def bronze_path_for(html: str) -> Path:
    content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
    return BRONZE_DIR / f"{content_hash}.html"


def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {}


def _save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def fetch(url: str) -> str:
    """Ambil HTML mentah dari url, simpan content-addressed ke Bronze, kembalikan isinya.

    Manifest (url -> content_hash) dicek dulu sebelum request: kalau url ini
    sudah pernah difetch, langsung kembalikan HTML yang sudah tersimpan tanpa
    request HTTP baru. Ini menghindari duplikasi Bronze akibat konten dinamis
    (timestamp, artikel terkait, dst) yang bikin hash HTML berubah tiap fetch
    walau artikelnya sendiri sama - lihat bronze_path_for() yang tetap hash
    dari konten, tidak berubah oleh manifest ini.
    """
    manifest = _load_manifest()

    cached = manifest.get(url)
    if cached is not None:
        cached_path = BRONZE_DIR / f"{cached['content_hash']}.html"
        if cached_path.exists():
            return cached_path.read_text(encoding="utf-8")

    if not _check_robots_allowed(url):
        raise PermissionError(f"robots.txt melarang fetch untuk: {url}")

    _respect_rate_limit(url)

    response = httpx.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=15.0, follow_redirects=True
    )
    response.raise_for_status()
    html = response.text

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    path = bronze_path_for(html)
    if not path.exists():
        path.write_text(html, encoding="utf-8")

    manifest[url] = {
        "content_hash": path.stem,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_manifest(manifest)

    return html


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/fetch.py <url>")
        sys.exit(1)

    target_url = sys.argv[1]
    fetched_html = fetch(target_url)
    saved_path = bronze_path_for(fetched_html)
    print(f"Fetched {len(fetched_html)} karakter dari {target_url}")
    print(f"Tersimpan di: {saved_path}")
