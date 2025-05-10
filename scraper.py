#!/usr/bin/env python3
import sys
import time
import requests
import psycopg2
import os
from lxml import html
from urllib.parse import urljoin

BASE_URL = "https://www.dtc-lease.nl"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "*/*",
}

# Pull DB credentials from environment or fallback defaults
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
    """
    Fetch the URL with retry/backoff logic for status 403/429 or Connection/Timeout errors.
    Returns `requests.Response` or None if all retries fail.
    """
    backoffs = [10, 30, 60, 120]
    attempt = 0
    while attempt < max_retries:
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            print(f"[fetch] GET {url} => {resp.status_code}")
            if resp.status_code in (403, 429):
                # 403 or 429 => wait + retry
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
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Retry on certain network errors
            if attempt < max_retries - 1:
                wait = backoffs[attempt]
                print(f"[warn] {type(e).__name__} => wait {wait}s, retry {url}...")
                time.sleep(wait)
                attempt += 1
            else:
                print(f"[error] {type(e).__name__} after {max_retries} tries => skip {url}")
                return None
    return None

def get_brand_links(session):
    """
    Scrape the /merken page to get all the brand/model "listing" links.
    (E.g. https://www.dtc-lease.nl/voorraad/audi/a1?lease_type=financial&entity=business).
    """
    url = urljoin(BASE_URL, "/merken")
    resp = robust_fetch(url, session)
    if not resp or not resp.ok:
        print(f"[error] Cannot fetch brand list => {url}")
        return []

    tree = html.fromstring(resp.text)
    # XPath for brand/model links:
    xp = '//main[@id="main-content"]//ul/li[@class="text-cta-1 ml-6 list-disc"]/a/@href'
    found_links = tree.xpath(xp)

    # Convert to absolute URLs:
    abs_links = [urljoin(BASE_URL, ln) for ln in found_links]
    return abs_links

def get_listing_links(page_url, session):
    """
    Given a brand/model listing URL (including &page=X if needed),
    return up to 16 product links from that page.
    If none => [], meaning "stop" for that brand/model.
    """
    resp = robust_fetch(page_url, session)
    if not resp or not resp.ok:
        return []

    tree = html.fromstring(resp.text)
    # Check if there's at least a 'product-result-1'
    first_sel = '//main[@id="main-content"]//a[@data-testid="product-result-1"]/@href'
    first_link = tree.xpath(first_sel)
    if not first_link:
        return []

    links = []
    # data-testid="product-result-0" through -16, etc. 
    # But typically they start at 1. We'll check 1..16
    for i in range(1, 17):
        xp = f'//main[@id="main-content"]//a[@data-testid="product-result-{i}"]/@href'
        found = tree.xpath(xp)
        if found:
            links.extend(found)

    abs_links = [urljoin(BASE_URL, ln) for ln in links]
    return abs_links

def parse_detail(detail_url, session):
    """
    Parse a single detail page, returning a dict with all relevant fields.
    If fails => return None.
    """
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
    if title:
        record["title"] = title[0].strip()

    subt = t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()')
    if subt:
        record["subtitle"] = subt[0].strip()

    # Lease price + term
    flp = t('//div[@data-testid="price-block"]//h2/text()')
    if flp:
        record["financial_lease_price"] = flp[0].strip()

    flt = t('//div[@data-testid="price-block"]//p[contains(@class,"info-sm") and contains(text(),"mnd")]/text()')
    if flt:
        record["financial_lease_term"] = flt[0].strip()

    # Advertentienummer
    adnum = t('//div[contains(@class,"p-sm") and contains(text(),"Advertentienummer")]/text()')
    if adnum:
        record["advertentienummer"] = adnum[0].split(":", 1)[-1].strip()

    # Helper function for specs
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

    # Address
    addr_sel = '//div[@class="flex justify-between"]/div/p[@class="text-p-sm font-light text-black tablet:text-p"]/text()'
    addr = t(addr_sel)
    if addr:
        record["address"] = addr[0].strip()

    # Images
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

    # 1) Gather all brand/model listing links
    brand_links = get_brand_links(session)
    print(f"[info] Found {len(brand_links)} brand-model listing links.")

    # 2) Collect all detail-page URLs from all brand links (with pagination)
    all_scraped_urls = set()
    for brand_link in brand_links:
        page = 1
        while True:
            if page == 1:
                page_url = brand_link
            else:
                page_url = f"{brand_link}&page={page}"

            print(f"[info] Fetching listing page => {page_url}")
            links = get_listing_links(page_url, session)
            if not links:
                print(f"[info] No links found on page {page}. Stop pagination for {brand_link}.")
                break

            for ln in links:
                all_scraped_urls.add(ln)

            print(f"[info] Found {len(links)} product links on page {page} (so far total {len(all_scraped_urls)})")

            page += 1
            # Safety check to avoid infinite loops:
            if page > 100:
                print("[warn] Reached 100 pages => stopping pagination for this brand/model.")
                break

            # Short delay between pages
            time.sleep(2)

    print(f"[info] Final total unique detail URLs scraped: {len(all_scraped_urls)}")

    # 3) Connect to DB and figure out which are "new" vs. "obsolete" vs. overlap
    try:
        conn = connect_db()
        cur = conn.cursor()

        # Fetch existing detail URLs from DB
        cur.execute("SELECT url FROM car_listings;")
        rows = cur.fetchall()
        existing_db_urls = set(r[0] for r in rows)
        print(f"[info] Currently have {len(existing_db_urls)} URLs in DB.")

        # Determine new and obsolete
        new_urls = all_scraped_urls - existing_db_urls
        obsolete_urls = existing_db_urls - all_scraped_urls
        overlap = all_scraped_urls & existing_db_urls

        print(f"[info] Overlap: {len(overlap)} URLs (no action needed).")
        print(f"[info] New: {len(new_urls)} URLs (need to scrape + insert).")
        print(f"[info] Obsolete: {len(obsolete_urls)} URLs (will delete).")

        # 3A) DELETE obsolete URLs
        # This also should cascade-delete rows in car_images if you have FK with ON DELETE CASCADE
        # Otherwise, manually delete from car_images first if needed.
        if obsolete_urls:
            print("[info] Deleting obsolete URLs from DB...")
            # Convert set -> list to pass to execute
            cur.execute(
                "DELETE FROM car_listings WHERE url = ANY(%s)",
                (list(obsolete_urls),)
            )
            conn.commit()
            print(f"[info] Deleted {cur.rowcount} obsolete car_listings.")

        # 3B) SCRAPE + INSERT for new URLs only
        if new_urls:
            print("[info] Scraping detail data for new URLs and inserting into DB...")
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
            # We'll use a dedicated session for detail scraping
            detail_session = requests.Session()

            for url in new_urls:
                print(f"[info] Parsing detail for new URL => {url}")
                rec = parse_detail(url, detail_session)
                if not rec:
                    # skip if parse failed
                    continue

                try:
                    cur.execute(insert_listings_sql, (
                        rec["url"],
                        rec["title"],
                        rec["subtitle"],
                        rec["financial_lease_price"],
                        rec["financial_lease_term"],
                        rec["advertentienummer"],
                        rec["merk"],
                        rec["model"],
                        rec["bouwjaar"],
                        rec["km_stand"],
                        rec["transmissie"],
                        rec["prijs"],
                        rec["brandstof"],
                        rec["btw_marge"],
                        rec["opties_accessoires"],
                        rec["address"]
                    ))
                    new_id = cur.fetchone()[0]

                    # Insert images
                    for img_url in rec["images"]:
                        cur.execute(insert_images_sql, (img_url, new_id))

                    conn.commit()
                    inserted_count += 1

                except Exception as insert_err:
                    print(f"[error] Insert failed for {url} => {insert_err}")
                    conn.rollback()

                # Small delay to be polite
                time.sleep(1)

            print(f"[info] Inserted {inserted_count} new listings into DB.")
        else:
            print("[info] No new URLs to insert.")

    except Exception as e:
        print(f"[error] DB error => {e}")
        sys.exit(1)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        print("[info] DB connection closed.")

    print("[info] Scraper finished successfully.")

if __name__ == "__main__":
    main()
