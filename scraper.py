#!/usr/bin/env python3
import sys
import time
import requests
import psycopg2
import os
from lxml import html
from urllib.parse import urljoin

# ---------- CONFIG ----------
BASE_URL = "https://www.dtc-lease.nl"
LISTING_URL_TEMPLATE = (
    "https://www.dtc-lease.nl/voorraad"
    "?lease_type=financial"
    "&voertuigen%%5Bpage%%5D={page}"
    "&voertuigen%%5BsortBy%%5D=voertuigen_created_at_desc"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "*/*",
}

DB_NAME = os.environ.get("DB_NAME", "neolease_db")
DB_USER = os.environ.get("DB_USER", "neolease_db_user")
DB_PASS = os.environ.get("DB_PASS", "DKuNZ0Z4OhuNKWvEFaAuWINgr7BfgyTE")
DB_HOST = os.environ.get("DB_HOST", "dpg-cvslkuvdiees73fiv97g-a.oregon-postgres.render.com")
DB_PORT = os.environ.get("DB_PORT", "5432")

MAX_PAGES = 90  # Prevent infinite loops
MAX_REPEAT_THRESHOLD = 2  # If we see the same links N times in a row => stop

# ---------- DB UTILS ----------
def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

# ---------- SCRAPING UTILS ----------
def robust_fetch(url, session, max_retries=3):
    """
    Fetch URL with basic retry/backoff for 403/429 or timeouts.
    Returns requests.Response or None if all fails.
    """
    backoffs = [10, 30, 120]  # seconds
    attempt = 0
    while attempt < max_retries:
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            print(f"[fetch] GET {url} => {resp.status_code}")
            # If we get 403/429, might be rate-limit => backoff
            if resp.status_code in (403, 429):
                if attempt < max_retries - 1:
                    wait = backoffs[attempt]
                    print(f"[warn] {resp.status_code} => wait {wait}s, retrying {url}...")
                    time.sleep(wait)
                    attempt += 1
                    continue
                else:
                    print(f"[error] {resp.status_code} after {max_retries} tries => skip {url}")
                    return None
            return resp
        except (requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait = backoffs[attempt]
                print(f"[warn] {type(e).__name__} => wait {wait}s, retrying {url}...")
                time.sleep(wait)
                attempt += 1
            else:
                print(f"[error] {type(e).__name__} after {max_retries} tries => skip {url}")
                return None
    return None

def get_listing_links(page, session):
    url = LISTING_URL_TEMPLATE.format(page=page)
    resp = robust_fetch(url, session)
    if not resp or not resp.ok:
        return []
    tree = html.fromstring(resp.text)

    # check if product-result-1 found => if not => no more
    first = tree.xpath('//main[@id="main-content"]//a[@data-testid="product-result-1"]/@href')
    if not first:
        return []

    links = []
    for i in range(1, 17):
        xp = f'//main[@id="main-content"]//a[@data-testid="product-result-{i}"]/@href'
        found = tree.xpath(xp)
        if found:
            links.extend(found)
    abs_links = [urljoin(BASE_URL, ln) for ln in links]
    return abs_links

def parse_detail(url, session):
    record = {
        "url": url,
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
        "images": []
    }
    resp = robust_fetch(url, session)
    if not resp or not resp.ok:
        print(f"[error] Cannot fetch detail => {url}")
        return None

    tree = html.fromstring(resp.text)
    def t(xp):
        return tree.xpath(xp)

    # Title, subtitle, fl price, term
    title = t('//h1[@class="h1-sm tablet:h1 text-trustful-1"]/text()')
    record["title"] = title[0].strip() if title else None

    subtitle = t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()')
    record["subtitle"] = subtitle[0].strip() if subtitle else None

    flp = t('//div[@data-testid="price-block"]//h2/text()')
    record["financial_lease_price"] = flp[0].strip() if flp else None

    flt = t('//div[@data-testid="price-block"]//p[contains(@class,"info-sm") and contains(text(),"mnd")]/text()')
    record["financial_lease_term"] = flt[0].strip() if flt else None

    # Advertentienummer
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

    # Opties & accessoires
    oa = t('//h2[contains(.,"Opties & Accessoires")]/following-sibling::ul/li/text()')
    if oa:
        cleaned = [x.strip() for x in oa if x.strip()]
        record["opties_accessoires"] = ", ".join(cleaned)

    # Address
    addr = t('//div[@class="flex justify-between"]/div/p[@class="text-p-sm font-light text-black tablet:text-p"]/text()')
    if addr:
        record["address"] = addr[0].strip()

    # Images
    imgs = t('//ul[@class="swiper-wrapper pb-10"]/li/img/@src')
    if imgs:
        for i in imgs:
            if i.startswith("/"):
                i = urljoin(BASE_URL, i)
            record["images"].append(i)

    return record

# ---------- MAIN ------------
def main():
    print("[info] Starting DTC scraper...")

    session = requests.Session()
    session.headers.update(HEADERS)

    all_listings = []

    seen_linksets = set()
    repeated_pages = 0

    page = 1
    while True:
        if page > MAX_PAGES:
            print(f"[warn] Reached page {page} > {MAX_PAGES} => stop scraping.")
            break

        print(f"[info] Listing page {page} ...")
        links = get_listing_links(page, session)
        if not links:
            print("[info] No more links => stop pagination.")
            break

        print(f"[info] Found {len(links)} links on page {page}.")
        linkset = frozenset(links)
        if linkset in seen_linksets:
            repeated_pages += 1
            print(f"[warn] This page's links were already seen => repeated_pages={repeated_pages}")
            if repeated_pages >= MAX_REPEAT_THRESHOLD:
                print("[warn] Repeated pages threshold reached => stop pagination.")
                break
        else:
            repeated_pages = 0
            seen_linksets.add(linkset)

        # Now parse each listing
        for idx, ln in enumerate(links, start=1):
            print(f"   -> detail: {ln}")
            rec = parse_detail(ln, session)
            if rec:
                all_listings.append(rec)
            time.sleep(1)  # small delay per detail

        page += 1
        time.sleep(2)  # small delay per page

    print(f"[info] Total listings scraped: {len(all_listings)}")

    if not all_listings:
        print("[error] No data scraped => skip DB update.")
        sys.exit(1)

    try:
        conn = connect_db()
        cur = conn.cursor()

        # TRUNCATE existing data
        print("[info] Clearing old data in DB (car_images + car_listings)...")
        cur.execute("TRUNCATE car_images;")
        cur.execute("TRUNCATE car_listings RESTART IDENTITY CASCADE;")
        conn.commit()

        # Insert new
        print("[info] Inserting new records...")
        insert_cl = """
        INSERT INTO car_listings (
            url, title, subtitle, financial_lease_price, financial_lease_term,
            advertentienummer, merk, model, bouwjaar, km_stand,
            transmissie, prijs, brandstof, btw_marge, opties_accessoires, address
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        insert_ci = """
        INSERT INTO car_images (image_url, car_listing_id)
        VALUES (%s, %s);
        """

        count_inserted = 0
        for item in all_listings:
            cur.execute(insert_cl, (
                item["url"],
                item["title"],
                item["subtitle"],
                item["financial_lease_price"],
                item["financial_lease_term"],
                item["advertentienummer"],
                item["merk"],
                item["model"],
                item["bouwjaar"],
                item["km_stand"],
                item["transmissie"],
                item["prijs"],
                item["brandstof"],
                item["btw_marge"],
                item["opties_accessoires"],
                item["address"]
            ))
            new_id = cur.fetchone()[0]

            # Insert image rows
            if item["images"]:
                for img_url in item["images"]:
                    cur.execute(insert_ci, (img_url, new_id))

            count_inserted += 1

        conn.commit()
        print(f"[info] Inserted {count_inserted} listings into DB successfully!")

    except Exception as e:
        print(f"[error] DB error => {e}")
        sys.exit(1)
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
        print("[info] DB connection closed.")


if __name__ == "__main__":
    main()
