import argparse
import csv
import hashlib
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import random

import requests
from bs4 import BeautifulSoup

import boto3

BASE_URL = "https://www.mirkvartir.ru/Москва/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    )
}
PROJECT_ROOT = Path(__file__).resolve().parents[3]



# Настройки для подключения к Docker-контейнеру
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000", # Порт для API
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)


BUCKET_NAME = "mirkvartir"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]


class BlockedBySiteError(RuntimeError):
    pass


def is_blocked_response(resp: requests.Response) -> bool:
    blocked_statuses = {403, 429, 503}
    if resp.status_code in blocked_statuses:
        return True
    body = resp.text.lower()
    blocked_markers = (
        "captcha",
        "access denied",
        "forbidden",
        "too many requests",
        "bot",
        "cloudflare",
    )
    return any(marker in body for marker in blocked_markers)


def fetch_with_retry(
    session: requests.Session,
    url: str,
    timeout: int = 25,
    retries: int = 4,
    base_pause: float = 0.7,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        try:
            resp = session.get(url, timeout=timeout)
            if is_blocked_response(resp):
                raise BlockedBySiteError(
                    f"Site blocked request (status={resp.status_code}) for {url}"
                )
            resp.raise_for_status()
            return resp
        except BlockedBySiteError:
            raise
        except requests.RequestException as err:
            last_error = err
            if attempt == retries:
                break
            backoff = base_pause * (2 ** (attempt - 1)) + random.uniform(0.2, 1.0)
            print(f"Retry {attempt}/{retries - 1} for {url}, sleeping {backoff:.2f}s")
            time.sleep(backoff)

    if last_error:
        raise last_error
    raise requests.RequestException(f"Failed to fetch {url}")


def to_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def to_float(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"\d+(?:[.,]\d+)?", value.replace(" ", ""))
    if not match:
        return None
    return float(match.group(0).replace(",", "."))


def floor_pair(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    match = re.search(r"(\d+)\s*/\s*(\d+)", value)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    tail = path.split("/")[-1] or hashlib.md5(url.encode("utf-8")).hexdigest()[:10]
    return re.sub(r"[^\w\-]+", "_", tail)


def extract_coordinates(html: str) -> tuple[float | None, float | None]:
    patterns = [
        r'"lat"\s*:\s*([0-9]+\.[0-9]+)\s*,\s*"lng"\s*:\s*([0-9]+\.[0-9]+)',
        r'"lat"\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*,\s*"lon"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
        r'"latitude"\s*:\s*([0-9]+\.[0-9]+)\s*,\s*"longitude"\s*:\s*([0-9]+\.[0-9]+)',
        r"point\s*:\s*\[\s*([0-9]+\.[0-9]+)\s*,\s*([0-9]+\.[0-9]+)\s*\]",
    ]
    for pattern in patterns:
        m = re.search(pattern, html)
        if m:
            return float(m.group(1)), float(m.group(2))
    return None, None


def extract_images(soup: BeautifulSoup, html: str, detail_url: str) -> list[str]:
    image_urls: set[str] = set()

    for img in soup.select("img"):
        for attr in ("data-src", "data-original", "src"):
            val = img.get(attr)
            if not val:
                continue
            if any(x in val.lower() for x in (".jpg", ".jpeg", ".png", ".webp")):
                image_urls.add(urljoin(detail_url, val))

    raw_urls = re.findall(r'https?://[^"\']+\.(?:jpg|jpeg|png|webp)(?:\?[^"\']*)?', html, re.I)
    for url in raw_urls:
        if "mirkvartir" in url:
            image_urls.add(url)

    bad_tokens = ("logo", "favicon", "sprite", "icon", "avatar")
    return sorted(u for u in image_urls if not any(t in u.lower() for t in bad_tokens))


def save_image_to_s3(session, url, listing_id, idx):
    """Качает фото и сразу льет в S3. Возвращает S3 URI """
    try:
        resp = fetch_with_retry(session, url, timeout=15, retries=3, base_pause=0.5)
        resp.raise_for_status()

        ext = Path(urlparse(url).path).suffix.lower() or ".jpg"
        s3_key = f"images/{listing_id}/{idx:03d}{ext}"  # Это Key объекта [cite: 47]

        # Загружаем байты [cite: 67, 68]
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=resp.content,
            ContentType="image/jpeg"
        )
        return f"s3://{BUCKET_NAME}/{s3_key}"  # Возвращаем URI [cite: 181]
    except Exception as e:
        print(f"S3 Upload Error: {e}")
        return None


def extract_detail(session: requests.Session, url: str, root_image_dir: Path, pause: float) -> dict:
    resp = fetch_with_retry(session, url, timeout=25, retries=4, base_pause=0.7)
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    title = soup.select_one("h1")
    description_node = (
        soup.select_one('[itemprop="description"]')
        or soup.select_one(".offer-description")
        or soup.select_one(".offer_text")
        or soup.select_one(".l-object-description")
        or soup.select_one(".m-description")
    )
    price_node = soup.select_one('[itemprop="price"]') or soup.select_one(".price")
    area_node = soup.find(string=re.compile(r"м²"))
    floor_node = soup.find(string=re.compile(r"\d+\s*/\s*\d+\s*этаж", re.I))

    lat, lon = extract_coordinates(html)
    image_urls = extract_images(soup, html, url)
    listing_id = slug_from_url(url)
    s3_images = []
    for i, img_url in enumerate(image_urls[:5], start=1):
        uri = save_image_to_s3(session, img_url, listing_id, i)
        if uri:
            s3_images.append(uri)
        time.sleep(pause)
    current_floor, total_floors = floor_pair(floor_node if isinstance(floor_node, str) else None)

    return {
        "id": listing_id,
        "url": url,
        "title": title.get_text(" ", strip=True) if title else None,
        "price_rub": to_int(price_node.get_text(" ", strip=True) if price_node else None),
        "area_m2": to_float(area_node if isinstance(area_node, str) else None),
        "floor_current": current_floor,
        "floor_total": total_floors,
        "latitude": lat,
        "longitude": lon,
        "description_text": description_node.get_text(" ", strip=True) if description_node else None,
        "images": s3_images,
    }


def listing_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links = set()
    for a in soup.select("a[href]"):
        href = a["href"].split("#")[0]
        full_url = urljoin(base_url, href)
        path = urlparse(full_url).path
        # На сайте карточки объявлений имеют путь вида "/355178963/".
        if re.fullmatch(r"/\d{6,}/?", path):
            links.add(full_url)
    return sorted(links)


def parse_pages(start_url: str, pages: int, pause: float, limit: int | None, out_csv: Path) -> list[dict]:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers.update({"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"})

    data_dir = PROJECT_ROOT / "DATA"
    image_root = data_dir / "images"
    data_dir.mkdir(parents=True, exist_ok=True)
    image_root.mkdir(parents=True, exist_ok=True)

    collected: list[dict] = []
    seen: set[str] = set()

    for page in range(1, pages + 1):
        page_url = start_url if page == 1 else f"{start_url}?p={page}"
        try:
            resp = fetch_with_retry(session, page_url, timeout=25, retries=4, base_pause=0.7)
        except BlockedBySiteError as err:
            print(f"Blocked on listing page, stopping parser: {err}")
            return collected
        soup = BeautifulSoup(resp.text, "html.parser")

        for url in listing_links(soup, start_url):
            if url in seen:
                continue
            seen.add(url)
            print(f"Parsing object: {url}")
            try:
                item = extract_detail(session, url, image_root, pause)
                collected.append(item)
                append_csv_row(item, out_csv)
                print(f"Saved object #{len(collected)}: id={item['id']}")
            except BlockedBySiteError as err:
                print(f"Blocked while parsing object, stopping parser: {err}")
                return collected
            except requests.RequestException:
                continue

            if limit and len(collected) >= limit:
                return collected
            time.sleep(pause + random.uniform(0.2, 1.0))

    return collected


def get_fieldnames() -> list[str]:
    return [
        "id",
        "url",
        "title",
        "price_rub",
        "area_m2",
        "floor_current",
        "floor_total",
        "latitude",
        "longitude",
        "description_text",
        "images_count",
        "images",
    ]


def normalize_row(item: dict) -> dict:
    row = dict(item)
    row["images_count"] = len(item.get("images", []))
    row["images"] = ";".join(item.get("images", []))
    return row


def append_csv_row(item: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = get_fieldnames()
    write_header = not out_path.exists() or out_path.stat().st_size == 0
    with out_path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(normalize_row(item))


def main() -> None:
    parser = argparse.ArgumentParser(description="Mirkvartir apartment parser")
    parser.add_argument("--url", default=BASE_URL, help="Start listing URL")
    parser.add_argument("--pages", type=int, default=400, help="How many listing pages to parse")
    parser.add_argument("--limit", type=int, default=3, help="Max apartments to collect")
    parser.add_argument("--pause", type=float, default=0.5, help="Pause between requests")
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output CSV path. By default creates a new DATA/apartments_YYYYMMDD_HHMMSS.csv",
    )
    args = parser.parse_args()

    if args.output:
        out_csv = Path(args.output)
        if not out_csv.is_absolute():
            out_csv = PROJECT_ROOT / out_csv
    else:
        out_csv = PROJECT_ROOT / "DATA" / f"apartments.csv"

    # Start with clean file so we always have a consistent stream result.
    if out_csv.exists():
        out_csv.unlink()

    items = parse_pages(args.url, args.pages, args.pause, args.limit, out_csv)
    print(f"Saved {len(items)} records to {out_csv}")


if __name__ == "__main__":
    main()
