#!/usr/bin/env python3
import sys
import time
import requests
import psycopg2
import os
from lxml import html
from urllib.parse import urljoin

BASE_URL = "https://www.dtc-lease.nl"
LISTING_URL_TEMPLATE = (
    "https://www.dtc-lease.nl/voorraad"
    "?lease_type=financial"
    "&voertuigen%5Bpage%5D={page}"
    "&voertuigen%5BsortBy%5D=voertuigen_created_at_desc"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "*/*",
}

# Pull DB credentials from environment or fallback:
DB_NAME = os.getenv("DB_NAME", "neolease_db")
DB_USER = os.getenv("DB_USER", "neolease_db_user")
DB_PASS = os.getenv("DB_PASS", "DKuNZ0Z4OhuNKWvEFaAuWINgr7BfgyTE")
DB_HOST = os.getenv("DB_HOST", "dpg-cvslkuvdiees73fiv97g-a.oregon-postgres.render.com")
DB_PORT = os.getenv("DB_PORT", "5432")

def connect_db():
    """Connect to Postgres with psycopg2."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def robust_fetch(url, session, max_retries=4):
    """Similar robust fetch as local version."""
    backoffs = [10, 30, 60, 120]
    attempt = 0
    while attempt < max_retries:
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            print(f"[fetch] GET {url} => {resp.status_code}")
            if resp.status_code in (403, 429):
                if attempt < max_retries - 1:
                    wait = backoffs[attempt]
                    print(f"[warn] {resp.status_code} => wait {wait}s, retry...")
                    time.sleep(wait)
                    attempt += 1
                    continue
                else:
                    print(f"[error] {resp.status_code} after {max_retries} tries => skip.")
                    return None
            return resp
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait = backoffs[attempt]
                print(f"[warn] {type(e).__name__} => wait {wait}s, retry {url}...")
                time.sleep(wait)
                attempt += 1
            else:
                print(f"[error] {type(e).__name__} after {max_retries} tries => skip {url}")
                return None
    return None

def get_listing_links(page_number, session):
    """Return up to 16 product links from the listing page. If none => []."""
    url = LISTING_URL_TEMPLATE.format(page=page_number)
    resp = robust_fetch(url, session)
    if not resp or not resp.ok:
        return []
    tree = html.fromstring(resp.text)

    first_sel = '//main[@id="main-content"]//a[@data-testid="product-result-1"]/@href'
    first_link = tree.xpath(first_sel)
    if not first_link:
        return []

    links = []
    for i in range(0, 16):
        xp = f'//main[@id="main-content"]//a[@data-testid="product-result-{i}"]/@href'
        found = tree.xpath(xp)
        if found:
            links.extend(found)

    abs_links = [urljoin(BASE_URL, ln) for ln in links]
    return abs_links

def parse_detail(detail_url, session):
    record = {
        "url": detail_url,
        "title": None,
        "subtitle": None,
        "financial_lease_price": None,
        "financial_lease_term": None,
        "advertentienummer": None,
        "merk": None,
        "model": None,
        "bouwjaar": None,
        "km_stand": None,
        "transmissie": None,
        "prijs": None,
        "brandstof": None,
        "btw_marge": None,
        "opties_accessoires": None,
        "address": None,
        "images": [],
    }
    resp = robust_fetch(detail_url, session)
    if not resp or not resp.ok:
        print(f"[error] Cannot fetch detail => {detail_url}")
        return None

    tree = html.fromstring(resp.text)
    t = lambda xp: tree.xpath(xp)

    # Title / subtitle
    title = t('//h1[@class="h1-sm tablet:h1 text-trustful-1"]/text()')
    if title: record["title"] = title[0].strip()

    subt = t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()')
    if subt: record["subtitle"] = subt[0].strip()

    flp = t('//div[@data-testid="price-block"]//h2/text()')
    if flp: record["financial_lease_price"] = flp[0].strip()

    flt = t('//div[@data-testid="price-block"]//p[contains(@class,"info-sm") and contains(text(),"mnd")]/text()')
    if flt: record["financial_lease_term"] = flt[0].strip()

    adnum = t('//div[contains(@class,"p-sm") and contains(text(),"Advertentienummer")]/text()')
    if adnum:
        record["advertentienummer"] = adnum[0].split(":",1)[-1].strip()

    def spec(label):
        xp = f'//div[@class="text-p-sm text-grey-1" and normalize-space(text())="{label}"]/following-sibling::div/text()'
        val = t(xp)
        return val[0].strip() if val else None

    record["merk"] = spec("Merk")
    record["model"] = spec("Model")
    record["bouwjaar"] = spec("Bouwjaar")
    record["km_stand"] = spec("Km stand")
    record["transmissie"] = spec("Transmissie")
    record["prijs"] = spec("Prijs")
    record["brandstof"] = spec("Brandstof")
    record["btw_marge"] = spec("Btw/marge")

    # Opties & Accessoires
    oa = t('//h2[contains(.,"Opties & Accessoires")]/following-sibling::ul/li/text()')
    if oa:
        cleaned = [x.strip() for x in oa if x.strip()]
        record["opties_accessoires"] = ", ".join(cleaned)

    addr_sel = '//div[@class="flex justify-between"]/div/p[@class="text-p-sm font-light text-black tablet:text-p"]/text()'
    addr = t(addr_sel)
    if addr:
        record["address"] = addr[0].strip()

    img_sel = '//ul[@class="swiper-wrapper pb-10"]/li/img/@src'
    imgs = t(img_sel)
    if imgs:
        for i in imgs:
            if i.startswith("/"):
                i = urljoin(BASE_URL, i)
            record["images"].append(i)

    return record

def main():
    print("[info] Starting DTC scraper (Render version) ...")
    session = requests.Session()

    all_listings = []
    page = 1
    while True:
        print(f"[info] Listing page {page} ...")
        links = get_listing_links(page, session)
        if not links:
            print(f"[info] No links found on page {page} => stop.")
            break

        print(f"[info] Found {len(links)} links on page {page}.")
        for ln in links:
            print(f"   -> detail: {ln}")
            rec = parse_detail(ln, session)
            if rec:
                all_listings.append(rec)
            time.sleep(1)

        page += 1
        if page > 100:
            print("[warn] Reached 100 pages => stopping.")
            break
        time.sleep(2)

    total_scraped = len(all_listings)
    print(f"[info] Total listings scraped: {total_scraped}")

    if total_scraped == 0:
        print("[warn] Scraped 0 listings. NOT wiping old DB data. Done.")
        sys.exit(0)

    # If we have some data, connect to DB and do the TRUNCATE + re-insert
    try:
        conn = connect_db()
        cur = conn.cursor()

        print("[info] Clearing old data in DB (car_images + car_listings)...")
        # We'll only do this because we have new data
        cur.execute("TRUNCATE car_images;")
        cur.execute("TRUNCATE car_listings RESTART IDENTITY CASCADE;")
        conn.commit()

        print("[info] Inserting new records...")
        insert_listings_sql = """
        INSERT INTO car_listings (
            url, title, subtitle, financial_lease_price, financial_lease_term,
            advertentienummer, merk, model, bouwjaar, km_stand,
            transmissie, prijs, brandstof, btw_marge, opties_accessoires,
            address
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        insert_images_sql = """
        INSERT INTO car_images (image_url, car_listing_id)
        VALUES (%s, %s);
        """

        inserted_count = 0
        for listing in all_listings:
            cur.execute(insert_listings_sql, (
                listing["url"],
                listing["title"],
                listing["subtitle"],
                listing["financial_lease_price"],
                listing["financial_lease_term"],
                listing["advertentienummer"],
                listing["merk"],
                listing["model"],
                listing["bouwjaar"],
                listing["km_stand"],
                listing["transmissie"],
                listing["prijs"],
                listing["brandstof"],
                listing["btw_marge"],
                listing["opties_accessoires"],
                listing["address"]
            ))
            new_id = cur.fetchone()[0]

            # Insert images
            for img_url in listing["images"]:
                cur.execute(insert_images_sql, (img_url, new_id))

            inserted_count += 1

        conn.commit()
        print(f"[info] Inserted {inserted_count} listings into DB successfully!")
    except Exception as e:
        print(f"[error] DB error => {e}")
        sys.exit(1)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        print("[info] DB connection closed.")

if __name__ == "__main__":
    main()
