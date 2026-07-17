"""Parser khusus Antara: HTML mentah -> field terstruktur.

Portal-specific (beda dari src/fetch.py yang generik) karena struktur
JSON-LD dan selector CSS di sini cuma berlaku untuk antaranews.com.
Input selalu HTML yang dibaca dari file Bronze, bukan hasil fetch langsung.
"""

import json

from bs4 import BeautifulSoup

BODY_SELECTOR = ".wrap__article-detail-content.post-content"
TITLE_SELECTOR = ".wrap__article-detail-title"


def _find_news_article_jsonld(soup: BeautifulSoup) -> dict:
    """Cari blok <script type="application/ld+json"> bertipe NewsArticle.

    Satu halaman Antara punya beberapa blok JSON-LD (WebSite, Organization,
    BreadcrumbList, dst) - kita cuma perlu yang NewsArticle.
    """
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        if data.get("@type") == "NewsArticle":
            return data
    return {}


def _meta_content(soup: BeautifulSoup, property_name: str) -> str:
    tag = soup.find("meta", property=property_name)
    return tag.get("content", "").strip() if tag else ""


def _extract_title(soup: BeautifulSoup, json_ld: dict) -> str:
    if json_ld.get("headline"):
        return json_ld["headline"].strip()
    if _meta_content(soup, "og:title"):
        return _meta_content(soup, "og:title")
    el = soup.select_one(TITLE_SELECTOR) or soup.find("h1")
    return el.get_text(strip=True) if el else ""


def _extract_body(soup: BeautifulSoup) -> str:
    """Body tidak tersedia di JSON-LD Antara - selalu diambil via CSS selector."""
    container = soup.select_one(BODY_SELECTOR)
    if container is None:
        return ""
    paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
    return "\n\n".join(text for text in paragraphs if text)


def _extract_url(soup: BeautifulSoup, json_ld: dict) -> str:
    return json_ld.get("url") or _meta_content(soup, "og:url")


def parse(html: str) -> dict:
    """Ekstrak title, body, publish_date, author, url dari HTML artikel Antara.

    publish_date dan author cuma tersedia lewat JSON-LD (tidak ada fallback
    CSS yang reliable untuk keduanya di template Antara) - kalau JSON-LD
    tidak ada, field ini akan kosong.
    """
    soup = BeautifulSoup(html, "html.parser")
    json_ld = _find_news_article_jsonld(soup)
    author = json_ld.get("author") or {}

    return {
        "title": _extract_title(soup, json_ld),
        "body": _extract_body(soup),
        "publish_date": json_ld.get("datePublished", ""),
        "author": author.get("name", ""),
        "url": _extract_url(soup, json_ld),
    }
