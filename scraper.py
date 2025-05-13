#!/usr/bin/env python3
"""
Fast DTC-Lease scraper for Render Cron
• 10-thread Phase-1 with summary counts
• 10-thread detail scrape
• Bulk INSERT
• Two-step delete (images ➜ listings)  — works with or without ON DELETE CASCADE
"""

import time, threading, requests, psycopg2, psycopg2.extras, concurrent.futures
from urllib.parse import urljoin
from lxml import html

# ─────────────────────────  CONFIG  ─────────────────────────
BASE_URL = "https://www.dtc-lease.nl"
HEADERS   = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS   = 10

DB_NAME = "neolease_db_kpz9"
DB_USER = "neolease_db_kpz9_user"
DB_PASS = "33H6QVFnAouvau72DlSjuKAMe5GdfviD"
DB_HOST = "dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com"
DB_PORT = "5432"

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT, sslmode="require", connect_timeout=10
    )

# ─────────────────────────  HTTP HELPERS  ─────────────────────────
def robust_fetch(url, session, max_retries=4):
    delays = [10, 30, 60, 120]
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code in (403, 429):
                if attempt < max_retries-1:
                    time.sleep(delays[attempt]); continue
                return None
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < max_retries-1:
                time.sleep(delays[attempt])
            else:
                return None
    return None

def get_brand_links(sess):
    r = robust_fetch(urljoin(BASE_URL, "/merken"), sess)
    if not r: return []
    tree = html.fromstring(r.text)
    rel  = tree.xpath('//main[@id="main-content"]//ul/li/a/@href')
    return [urljoin(BASE_URL, x) for x in rel]

def get_listing_links(page_url, sess):
    r = robust_fetch(page_url, sess)
    if not r: return []
    tree = html.fromstring(r.text)
    if not tree.xpath('//a[@data-testid="product-result-1"]'):
        return []
    links=[]
    for i in range(1,17):
        links += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL, x) for x in links]

# ─────────────────────────  PHASE 1  ─────────────────────────
def scrape_one_brand(brand_url, out_set, lock):
    sess = requests.Session()
    page, pages, cars = 1, 0, 0
    while True:
        url = f"{brand_url}&page={page}" if page>1 else brand_url
        links = get_listing_links(url, sess)
        if not links: break
        with lock:
            out_set.update(links)
        cars  += len(links)
        pages += 1
        page  += 1
        if page>100: break
        time.sleep(0.5)
    print(f"[phase1] done {brand_url} pages={pages} details={cars}", flush=True)

def collect_detail_urls():
    sess = requests.Session()
    brands = get_brand_links(sess)
    print(f"[phase1] {len(brands)} brand-model listing links", flush=True)
    all_urls, lock = set(), threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        pool.map(lambda b: scrape_one_brand(b, all_urls, lock), brands)
    print(f"[phase1] total detail URLs = {len(all_urls)}", flush=True)
    return all_urls

# ─────────────────────────  DETAIL PARSE  ─────────────────────────
def parse_detail(url):
    sess = requests.Session()
    r = robust_fetch(url, sess)
    if not r: return None
    t = html.fromstring(r.text).xpath
    spec = lambda lbl:(t(f'//div[normalize-space(text())="{lbl}"]/following-sibling::div/text()') or [None])[0]
    rec = {
        "url":url,
        "title":(t('//h1/text()') or [None])[0],
        "subtitle":(t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()') or [None])[0],
        "financial_lease_price":(t('//div[@data-testid="price-block"]//h2/text()') or [None])[0],
        "financial_lease_term":(t('//div[@data-testid="price-block"]//p[contains(text(),"mnd")]/text()') or [None])[0],
        "advertentienummer":(t('//div[contains(text(),"Advertentienummer")]/text()') or [None])[0],
        "merk":spec("Merk"), "model":spec("Model"), "bouwjaar":spec("Bouwjaar"),
        "km_stand":spec("Km stand"), "transmissie":spec("Transmissie"),
        "prijs":spec("Prijs"), "brandstof":spec("Brandstof"), "btw_marge":spec("Btw/marge"),
        "opties_accessoires":", ".join([x.strip() for x in t('//h2[contains(.,"Opties")]/following-sibling::ul/li/text()')]) or None,
        "address":(t('//div[@class="flex justify-between"]/div/p/text()') or [None])[0],
        "images":[urljoin(BASE_URL,src) if src.startswith('/') else src
                  for src in t('//ul[@class="swiper-wrapper pb-10"]/li/img/@src')]
    }
    return rec

# ─────────────────────────  MAIN  ─────────────────────────
def main():
    print("[info] DTC cron scraper starting …", flush=True)

    all_urls = collect_detail_urls()

    conn, cur = connect_db(), None
    try:
        cur = conn.cursor()
        cur.execute("SELECT url FROM car_listings;")
        existing = {r[0] for r in cur.fetchall()}
        new_urls = list(all_urls - existing)
        obsolete_urls = list(existing - all_urls)
        print(f"[info] overlap={len(all_urls & existing)}, new={len(new_urls)}, obsolete={len(obsolete_urls)}", flush=True)

        # ---------- safe delete ----------
        if obsolete_urls:
            cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete_urls,))
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
            cur.execute("DELETE FROM car_listings WHERE url = ANY(%s)", (obsolete_urls,))
            conn.commit()
            print(f"[info] deleted {cur.rowcount} obsolete listings", flush=True)

        # ---------- scrape new ----------
        if new_urls:
            print("[info] scraping new URLs in parallel …", flush=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
                records = list(filter(None, pool.map(parse_detail, new_urls)))
            print(f"[info] successfully parsed {len(records)} new records", flush=True)

            if records:
                listing_rows = [(r["url"],r["title"],r["subtitle"],r["financial_lease_price"],
                                 r["financial_lease_term"],r["advertentienummer"],r["merk"],
                                 r["model"],r["bouwjaar"],r["km_stand"],r["transmissie"],
                                 r["prijs"],r["brandstof"],r["btw_marge"],
                                 r["opties_accessoires"],r["address"]) for r in records]

                insert_listings = """
                    INSERT INTO car_listings (
                      url,title,subtitle,financial_lease_price,financial_lease_term,
                      advertentienummer,merk,model,bouwjaar,km_stand,
                      transmissie,prijs,brandstof,btw_marge,opties_accessoires,address
                    ) VALUES %s RETURNING id;
                """
                psycopg2.extras.execute_values(cur, insert_listings, listing_rows, page_size=500)
                ids = [r[0] for r in cur.fetchall()]

                img_rows = [(img, idx) for rec, idx in zip(records, ids) for img in rec["images"]]
                if img_rows:
                    psycopg2.extras.execute_values(cur,
                        "INSERT INTO car_images (image_url,car_listing_id) VALUES %s",
                        img_rows, page_size=1000)

                conn.commit()
                print(f"[info] added {len(listing_rows)} listings + {len(img_rows)} images", flush=True)

        print("[info] scraper finished OK", flush=True)

    finally:
        if cur: cur.close()
        conn.close()

if __name__ == "__main__":
    main()
