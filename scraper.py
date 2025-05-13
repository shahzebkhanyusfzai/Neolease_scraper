#!/usr/bin/env python3
"""
DTC-Lease scraper  •  10-slice worker model  •  span-based pagination
"""

import math, re, threading, time, concurrent.futures
import requests, psycopg2, psycopg2.extras
from urllib.parse import urljoin
from lxml import html

# ─────────────────────────  CONFIG  ─────────────────────────
BASE_URL = "https://www.dtc-lease.nl"
HEADERS  = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS  = 10      # slices & detail threads

DB_NAME  = "neolease_db_kpz9"
DB_USER  = "neolease_db_kpz9_user"
DB_PASS  = "33H6QVFnAouvau72DlSjuKAMe5GdfviD"
DB_HOST  = "dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com"
DB_PORT  = "5432"

# ─────────────────────────  DB  ─────────────────────────
def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST,  port=DB_PORT, sslmode="require", connect_timeout=10
    )

# ─────────────────────────  HTTP  ─────────────────────────
def robust_fetch(url, sess, tries=4):
    back = [10, 30, 60, 120]
    for a in range(tries):
        try:
            r = sess.get(url, headers=HEADERS, timeout=15)
            if r.status_code in (403, 429):
                if a < tries-1: time.sleep(back[a]); continue
                return None
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if a < tries-1: time.sleep(back[a])
    return None

def get_brand_links(sess):
    r = robust_fetch(urljoin(BASE_URL, "/merken"), sess)
    if not r: return []
    tree = html.fromstring(r.text)
    rel  = tree.xpath('//main[@id="main-content"]//ul/li/a/@href')
    return [urljoin(BASE_URL, u) for u in rel]

def extract_total(tree):
    """
    Returns integer inside “Toon resultaten (489)”
    or None if not found.
    """
    txts = tree.xpath('//span[contains(text(),"Toon resultaten")]/text()')
    if not txts: return None
    m = re.search(r'\((\d+)\)', txts[0])
    return int(m.group(1)) if m else None

def listing_links(tree):
    links=[]
    for i in range(16):
        links += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL, u) for u in links]

def scrape_listing_page(url, sess):
    r = robust_fetch(url, sess)
    if not r: return [], None
    tree = html.fromstring(r.text)
    if not tree.xpath('//a[@data-testid="product-result-1"]'):
        return [], None
    return listing_links(tree), extract_total(tree)

# ─────────────────────────  SLICE WORKER  ─────────────────────────
def scrape_slice(slice_id, brand_subset):
    sess = requests.Session()
    detail = set()
    for brand_url in brand_subset:
        base = f"{brand_url}?lease_type=financial&entity=business"
        page, cars, pages = 1, 0, 0

        # page 1
        links, total = scrape_listing_page(base, sess)
        if links:
            detail.update(links)
            cars  += len(links); pages += 1
        total_pages = math.ceil(total/16) if total else None

        # pages 2+
        while True:
            page += 1
            if total_pages and page > total_pages: break
            url = f"{base}&page={page}"
            links, _ = scrape_listing_page(url, sess)
            if not links:
                break
            detail.update(links)
            cars  += len(links); pages += 1
            time.sleep(0.5)
        print(f"[slice{slice_id}] {brand_url}  pages={pages}  details={cars}", flush=True)
    print(f"[slice{slice_id}] finished  total details={len(detail)}", flush=True)
    return detail

def collect_detail_urls():
    sess   = requests.Session()
    brands = get_brand_links(sess)
    print(f"[phase1] brand-models = {len(brands)}", flush=True)

    chunk  = math.ceil(len(brands)/WORKERS)
    slices = [brands[i:i+chunk] for i in range(0, len(brands), chunk)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        all_sets = pool.map(lambda t: scrape_slice(*t),
                            ((i,s) for i,s in enumerate(slices,1)))
    all_urls = set().union(*all_sets)
    print(f"[phase1] GRAND TOTAL detail URLs = {len(all_urls)}", flush=True)
    return all_urls

# ─────────────────────────  DETAIL SCRAPER  ─────────────────────────
def parse_detail(url):
    sess = requests.Session()
    r = robust_fetch(url, sess)
    if not r: return None
    tree = html.fromstring(r.text); t = tree.xpath
    spec = lambda lbl:(t(f'//div[normalize-space(text())="{lbl}"]/following-sibling::div/text()') or [None])[0]
    rec  = {
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
    print("[info] scraper start", flush=True)
    all_urls = collect_detail_urls()

    conn, cur = connect_db(), None
    try:
        cur = conn.cursor()
        cur.execute("SELECT url FROM car_listings;")
        existing = {r[0] for r in cur.fetchall()}
        new_urls = list(all_urls - existing)
        obsolete = list(existing - all_urls)
        print(f"[info] overlap={len(all_urls & existing)}  new={len(new_urls)}  obsolete={len(obsolete)}", flush=True)

        # safe delete
        if obsolete:
            cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
            cur.execute("DELETE FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            conn.commit()
            print(f"[info] deleted {cur.rowcount} obsolete listings", flush=True)

        # scrape new
        if new_urls:
            print("[info] scraping new detail pages…", flush=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
                records = list(filter(None, pool.map(parse_detail, new_urls)))
            print(f"[info] parsed {len(records)} new records", flush=True)

            if records:
                listing_rows = [
                    (r["url"], r["title"], r["subtitle"], r["financial_lease_price"],
                     r["financial_lease_term"], r["advertentienummer"], r["merk"],
                     r["model"], r["bouwjaar"], r["km_stand"], r["transmissie"],
                     r["prijs"], r["brandstof"], r["btw_marge"],
                     r["opties_accessoires"], r["address"])
                    for r in records
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO car_listings (
                      url,title,subtitle,financial_lease_price,financial_lease_term,
                      advertentienummer,merk,model,bouwjaar,km_stand,
                      transmissie,prijs,brandstof,btw_marge,opties_accessoires,address
                    ) VALUES %s RETURNING id
                    """,
                    listing_rows, page_size=500
                )
                ids = [r[0] for r in cur.fetchall()]

                img_rows = [(img, lid) for rec, lid in zip(records, ids) for img in rec["images"]]
                if img_rows:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO car_images (image_url,car_listing_id) VALUES %s",
                        img_rows, page_size=1000
                    )
                conn.commit()
                print(f"[info] inserted {len(listing_rows)} listings + {len(img_rows)} images", flush=True)

        print("[info] scraper done", flush=True)

    finally:
        if cur: cur.close()
        conn.close()

if __name__ == "__main__":
    main()
