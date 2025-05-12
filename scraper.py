#!/usr/bin/env python3
"""
Fast DTC-Lease scraper for Render Cron
– 10-thread pagination (Phase 1)
– 10-thread detail scraping (Phase 3)
– Bulk DB insert
Hard-coded DB creds (same as legacy script).
"""
import sys, os, time, threading, requests, psycopg2, psycopg2.extras, concurrent.futures
from urllib.parse import urljoin
from lxml import html

# ────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────────────────────────────────
BASE_URL = "https://www.dtc-lease.nl"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "*/*",
}
WORKERS = 10        # number of concurrent threads for both phases

# Hard-coded DB creds (legacy style)
DB_NAME = "neolease_db"
DB_USER = "neolease_db_user"
DB_PASS = "DKuNZ0Z4OhuNKWvEFaAuWINgr7BfgyTE"
DB_HOST = "dpg-cvslkuvdiees73fiv97g-a.oregon-postgres.render.com"
DB_PORT = "5432"

# ────────────────────────────────────────────────────────────────────────────
# DB CONNECTION
# ────────────────────────────────────────────────────────────────────────────
def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

# ────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ────────────────────────────────────────────────────────────────────────────
def robust_fetch(url, session, max_retries=4):
    backoff = [10, 30, 60, 120]
    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            if resp.status_code in (403, 429):
                if attempt < max_retries - 1:
                    time.sleep(backoff[attempt])
                    continue
                return None
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(backoff[attempt])
            else:
                return None
    return None

def get_brand_links(session):
    resp = robust_fetch(urljoin(BASE_URL, "/merken"), session)
    if not resp:
        return []
    tree = html.fromstring(resp.text)
    rel = tree.xpath(
        '//main[@id="main-content"]//ul/li[@class="text-cta-1 ml-6 list-disc"]/a/@href'
    )
    return [urljoin(BASE_URL, ln) for ln in rel]

def get_listing_links(page_url, session):
    resp = robust_fetch(page_url, session)
    if not resp:
        return []
    tree = html.fromstring(resp.text)
    if not tree.xpath('//a[@data-testid="product-result-1"]'):
        return []
    links = []
    for i in range(1, 17):
        links += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL, ln) for ln in links]

# ────────────────────────────────────────────────────────────────────────────
# PHASE 1: PARALLEL COLLECTION OF DETAIL URLs
# ────────────────────────────────────────────────────────────────────────────
def scrape_one_brand(brand_link, out_set, lock):
    """Walk all pages for one brand-model link and push detail URLs into shared set."""
    session = requests.Session()
    page = 1
    while True:
        page_url = f"{brand_link}&page={page}" if page > 1 else brand_link
        links = get_listing_links(page_url, session)
        if not links:
            break
        with lock:
            out_set.update(links)
        page += 1
        if page > 100:
            break
        time.sleep(0.5)            # polite delay
    print(f"[phase1] done: {brand_link}")

def collect_detail_urls():
    """Return a set of ALL detail URLs using WORKERS threads."""
    sess = requests.Session()
    brand_links = get_brand_links(sess)
    print(f"[phase1] {len(brand_links)} brand-model listing links")
    all_urls, lock = set(), threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(scrape_one_brand, bl, all_urls, lock) for bl in brand_links]
        concurrent.futures.wait(futures)
    print(f"[phase1] total detail URLs = {len(all_urls)}")
    return all_urls

# ────────────────────────────────────────────────────────────────────────────
# DETAIL PARSER
# ────────────────────────────────────────────────────────────────────────────
def parse_detail(detail_url):
    session = requests.Session()
    resp = robust_fetch(detail_url, session)
    if not resp:
        return None
    tree = html.fromstring(resp.text)
    t = tree.xpath
    spec = lambda lbl: (
        t(f'//div[@class="text-p-sm text-grey-1" and normalize-space(text())="{lbl}"]/'
          'following-sibling::div/text()') or [None]
    )[0]
    record = {
        "url": detail_url,
        "title": (t('//h1[@class="h1-sm tablet:h1 text-trustful-1"]/text()') or [None])[0],
        "subtitle": (
            t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()') or [None]
        )[0],
        "financial_lease_price": (
            t('//div[@data-testid="price-block"]//h2/text()') or [None]
        )[0],
        "financial_lease_term": (
            t('//div[@data-testid="price-block"]//p[contains(@class,"info-sm") and contains(text(),"mnd")]/text()') or [None]
        )[0],
        "advertentienummer": (
            t('//div[contains(@class,"p-sm") and contains(text(),"Advertentienummer")]/text()') or [None]
        )[0],
        "merk": spec("Merk"),
        "model": spec("Model"),
        "bouwjaar": spec("Bouwjaar"),
        "km_stand": spec("Km stand"),
        "transmissie": spec("Transmissie"),
        "prijs": spec("Prijs"),
        "brandstof": spec("Brandstof"),
        "btw_marge": spec("Btw/marge"),
        "opties_accessoires": ", ".join(
            [x.strip() for x in t('//h2[contains(.,"Opties")]/following-sibling::ul/li/text()')]
        ) or None,
        "address": (
            t('//div[@class="flex justify-between"]/div/p[@class="text-p-sm font-light text-black tablet:text-p"]/text()') or [None]
        )[0],
        "images": [
            urljoin(BASE_URL, src) if src.startswith("/") else src
            for src in t('//ul[@class="swiper-wrapper pb-10"]/li/img/@src')
        ],
    }
    return record

# ────────────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────────────
def main():
    print("[info] DTC cron scraper starting …")

    # ---------- Phase 1 : parallel build of URL universe ----------
    all_urls = collect_detail_urls()

    # ---------- Phase 2 : DB diff ----------
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT url FROM car_listings;")
    existing = {r[0] for r in cur.fetchall()}
    new_urls = list(all_urls - existing)
    obsolete_urls = list(existing - all_urls)

    print(
        f"[info] overlap={len(all_urls & existing)}, new={len(new_urls)}, "
        f"obsolete={len(obsolete_urls)}"
    )

    if obsolete_urls:
        cur.execute("DELETE FROM car_listings WHERE url = ANY(%s)", (obsolete_urls,))
        conn.commit()
        print(f"[info] deleted {cur.rowcount} obsolete listings")

    # ---------- Phase 3 : parallel detail scraping ----------
    if new_urls:
        print("[info] scraping new URLs in parallel …")
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            records = list(pool.map(parse_detail, new_urls))
        records = [r for r in records if r]
        print(f"[info] successfully parsed {len(records)} new records")

        # ---------- Phase 4 : bulk insert listings ----------
        if records:
            listing_tuples = [
                (
                    r["url"], r["title"], r["subtitle"], r["financial_lease_price"],
                    r["financial_lease_term"], r["advertentienummer"], r["merk"],
                    r["model"], r["bouwjaar"], r["km_stand"], r["transmissie"],
                    r["prijs"], r["brandstof"], r["btw_marge"],
                    r["opties_accessoires"], r["address"]
                )
                for r in records
            ]
            insert_listings = """
                INSERT INTO car_listings (
                    url, title, subtitle, financial_lease_price, financial_lease_term,
                    advertentienummer, merk, model, bouwjaar, km_stand,
                    transmissie, prijs, brandstof, btw_marge, opties_accessoires, address
                )
                VALUES %s
                RETURNING id;
            """
            psycopg2.extras.execute_values(
                cur, insert_listings, listing_tuples, page_size=500
            )
            new_ids = [row[0] for row in cur.fetchall()]

            # ---------- Phase 5 : bulk insert images ----------
            image_rows = []
            for rec, listing_id in zip(records, new_ids):
                image_rows.extend([(img, listing_id) for img in rec["images"]])

            if image_rows:
                insert_imgs = "INSERT INTO car_images (image_url, car_listing_id) VALUES %s"
                psycopg2.extras.execute_values(cur, insert_imgs, image_rows, page_size=1000)

            conn.commit()
            print(
                f"[info] added {len(listing_tuples)} listings "
                f"+ {len(image_rows)} images (bulk mode)"
            )

    cur.close()
    conn.close()
    print("[info] scraper finished OK")

if __name__ == "__main__":
    main()
