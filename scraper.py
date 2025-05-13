#!/usr/bin/env python3
"""
DTC-Lease scraper  –  10-slice workers
* span-based pagination
* chunked bulk inserts   (LISTING_CHUNK = 2 000, IMAGE_CHUNK = 10 000)
* NO server-side image filtering – front-end JS will hide 404s
"""

import math, re, time, concurrent.futures, requests, psycopg2, psycopg2.extras
from urllib.parse import urljoin
from lxml import html

# ───────────────────────── CONFIG ─────────────────────────
BASE_URL     = "https://www.dtc-lease.nl"
HEADERS      = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS      = 10

LISTING_CHUNK = 2_000          # how many listings per execute_values()
IMAGE_CHUNK   = 10_000         # how many image rows per execute_values()

DB_NAME = "neolease_db_kpz9"
DB_USER = "neolease_db_kpz9_user"
DB_PASS = "33H6QVFnAouvau72DlSjuKAMe5GdfviD"
DB_HOST = "dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com"
DB_PORT = "5432"


def db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        sslmode="require",
    )


# ───────────────────────── HTTP helpers ─────────────────────────
def http(url: str, sess: requests.Session, tries: int = 3):
    """robust GET with small exponential back-off"""
    for i in range(tries):
        try:
            r = sess.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            if r.status_code in (403, 429) and i < tries - 1:
                time.sleep((i + 1) * 10)
                continue
            return r
        except (requests.Timeout, requests.ConnectionError):
            if i < tries - 1:
                time.sleep((i + 1) * 10)
    return None


def total_results(tree: html.HtmlElement):
    """pulls integer from “Toon resultaten (489)” span, else None"""
    t = tree.xpath('//span[contains(text(),"Toon resultaten")]/text()')
    if t and (m := re.search(r"\((\d+)\)", t[0])):
        return int(m.group(1))
    return None


def list_links(tree: html.HtmlElement):
    """return up to 16 detail links on a listing page"""
    links = []
    for i in range(16):
        links += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL, u) for u in links]


# ───────────────────────── slice worker ─────────────────────────
def slice_worker(idx: int, brands: list[str]):
    sess = requests.Session()
    all_links = set()

    for b in brands:
        base = f"{b}?lease_type=financial&entity=business"
        r = http(base, sess)
        pages = cars = 0
        if not r:
            continue

        tree = html.fromstring(r.text)
        L = list_links(tree)
        all_links.update(L)
        pages += 1
        cars += len(L)

        max_pages = math.ceil(total_results(tree) / 16) if total_results(tree) else None
        p = 1
        while True:
            p += 1
            if max_pages and p > max_pages:
                break
            r = http(f"{base}&page={p}", sess)
            if not r or "product-result-1" not in r.text:
                break
            L = list_links(html.fromstring(r.text))
            if not L:
                break
            all_links.update(L)
            cars += len(L)
            pages += 1
            time.sleep(0.3)

        print(f"[slice{idx}] {b} pages={pages} cars={cars}", flush=True)

    print(f"[slice{idx}] done total={len(all_links)}", flush=True)
    return all_links


def collect_links():
    sess = requests.Session()
    brands = [
        urljoin(BASE_URL, u)
        for u in html.fromstring(http(urljoin(BASE_URL, "/merken"), sess).text).xpath(
            "//main//ul/li/a/@href"
        )
    ]
    print("[phase1] brands", len(brands), flush=True)

    chunk = math.ceil(len(brands) / WORKERS)
    slices = [brands[i : i + chunk] for i in range(0, len(brands), chunk)]

    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        result_sets = pool.map(
            lambda t: slice_worker(*t), ((i + 1, s) for i, s in enumerate(slices))
        )

    links = set().union(*result_sets)
    print("[phase1] GRAND", len(links), flush=True)
    return links


# ───────────────────────── detail scrape ─────────────────────────
def scrape_detail(url: str):
    sess = requests.Session()
    r = http(url, sess)
    if not r:
        return None

    tree = html.fromstring(r.text)
    t = tree.xpath
    get = (
        lambda lbl: (
            t(
                f'//div[normalize-space(text())="{lbl}"]/following-sibling::div/text()'
            )
            or [None]
        )[0]
    )

    images = [
        urljoin(BASE_URL, src) if src.startswith("/") else src
        for src in t('//ul[@class="swiper-wrapper pb-10"]/li/img/@src')
    ]

    return {
        "url": url,
        "title": (t("//h1/text()") or [None])[0],
        "subtitle": (
            t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()')
            or [None]
        )[0],
        "financial_lease_price": (
            t('//div[@data-testid="price-block"]//h2/text()') or [None]
        )[0],
        "financial_lease_term": (
            t(
                '//div[@data-testid="price-block"]//p[contains(text(),"mnd")]/text()'
            )
            or [None]
        )[0],
        "advertentienummer": (
            t('//div[contains(text(),"Advertentienummer")]/text()') or [None]
        )[0],
        "merk": get("Merk"),
        "model": get("Model"),
        "bouwjaar": get("Bouwjaar"),
        "km_stand": get("Km stand"),
        "transmissie": get("Transmissie"),
        "prijs": get("Prijs"),
        "brandstof": get("Brandstof"),
        "btw_marge": get("Btw/marge"),
        "opties_accessoires": ", ".join(
            [x.strip() for x in t('//h2[contains(.,"Opties")]/following-sibling::ul/li/text()')]
        )
        or None,
        "address": (t('//div[@class="flex justify-between"]/div/p/text()') or [None])[0],
        "images": images,
    }


# ───────────────────────── insert helpers ─────────────────────────
INSERT_LISTINGS = """
    INSERT INTO car_listings (
      url,title,subtitle,financial_lease_price,financial_lease_term,
      advertentienummer,merk,model,bouwjaar,km_stand,
      transmissie,prijs,brandstof,btw_marge,opties_accessoires,address
    ) VALUES %s RETURNING id;
"""
INSERT_IMAGES = "INSERT INTO car_images (image_url,car_listing_id) VALUES %s"


def bulk_insert(cur, recs):
    for i in range(0, len(recs), LISTING_CHUNK):
        chunk = recs[i : i + LISTING_CHUNK]

        listing_rows = [
            (
                r["url"],
                r["title"],
                r["subtitle"],
                r["financial_lease_price"],
                r["financial_lease_term"],
                r["advertentienummer"],
                r["merk"],
                r["model"],
                r["bouwjaar"],
                r["km_stand"],
                r["transmissie"],
                r["prijs"],
                r["brandstof"],
                r["btw_marge"],
                r["opties_accessoires"],
                r["address"],
            )
            for r in chunk
        ]
        psycopg2.extras.execute_values(
            cur, INSERT_LISTINGS, listing_rows, page_size=500
        )
        ids = [row[0] for row in cur.fetchall()]

        img_rows = [
            (img, lid) for rec, lid in zip(chunk, ids) for img in rec["images"]
        ]
        # possibly many images → chunk those too
        for j in range(0, len(img_rows), IMAGE_CHUNK):
            psycopg2.extras.execute_values(
                cur, INSERT_IMAGES, img_rows[j : j + IMAGE_CHUNK], page_size=1000
            )


# ───────────────────────── main ─────────────────────────
def main():
    print("[info] start", flush=True)
    links = collect_links()

    conn, cur = db(), None
    try:
        cur = conn.cursor()
        cur.execute("SELECT url FROM car_listings")
        existing = {u for (u,) in cur.fetchall()}

        new_urls = list(links - existing)
        obsolete_urls = list(existing - links)
        print(f"[info] new={len(new_urls)} obsolete={len(obsolete_urls)}", flush=True)

        # remove obsolete listings + images
        if obsolete_urls:
            cur.execute(
                "SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete_urls,)
            )
            obsolete_ids = [i for (i,) in cur.fetchall()]
            if obsolete_ids:
                cur.execute(
                    "DELETE FROM car_images WHERE car_listing_id = ANY(%s)",
                    (obsolete_ids,),
                )
            cur.execute(
                "DELETE FROM car_listings WHERE url = ANY(%s)", (obsolete_urls,)
            )
            conn.commit()
            print("[info] obsolete deleted", flush=True)

        # scrape & insert new listings
        if new_urls:
            print("[info] scraping details …", flush=True)
            with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
                records = list(filter(None, pool.map(scrape_detail, new_urls)))
            print(f"[info] parsed {len(records)} records — inserting", flush=True)
            bulk_insert(cur, records)
            conn.commit()
            print("[info] inserted OK", flush=True)

        print("[info] scraper done", flush=True)

    finally:
        if cur:
            cur.close()
        conn.close()


if __name__ == "__main__":
    main()
