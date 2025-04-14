import os
import csv
import sys
import time
import requests
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
    "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
    "Referer": "https://www.dtc-lease.nl/"
}

COOKIES = {
    # If special cookies or session data is required, set them here
}

CSV_COLUMNS = [
    "URL", "Title", "Subtitle", "Financial Lease Price", "Financial Lease Term",
    "Advertentienummer", "Merk", "Model", "Bouwjaar", "Km stand",
    "Transmissie", "Prijs", "Brandstof", "Btw/marge", "Opties & Accessoires",
    "Address", "Images"
]

def robust_fetch(session, url, max_retries=4):
    """
    Fetch a URL with backoff if we receive:
      - status 403 or 429 (possible block or rate limit)
      - requests exceptions like timeouts, connection errors
    We'll do up to 'max_retries' attempts with exponential-like backoff
    e.g. 10s, 30s, 2m, 5m, etc.

    If all attempts fail, return None so we can skip that page.
    """
    backoff_sec = [10, 30, 120, 300]  # example backoff intervals
    attempt = 0
    while attempt < max_retries:
        try:
            response = session.get(url, headers=HEADERS, cookies=COOKIES, timeout=20)
            print(f"GET {url} => {response.status_code}")

            # If we got 403 or 429, back off
            if response.status_code in (403, 429):
                if attempt < max_retries - 1:
                    wait = backoff_sec[attempt]
                    print(f"[WARN] Status {response.status_code} => sleeping {wait}s before retry...")
                    time.sleep(wait)
                    attempt += 1
                    continue
                else:
                    print(f"[ERROR] {response.status_code} after max retries => skipping {url}")
                    return None

            # If we got a normal OK or other code, just return it
            if response.ok:
                return response
            else:
                print(f"[WARN] Unexpected status {response.status_code} => returning anyway.")
                return response

        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError) as e:
            # handle timeouts or connection errors
            if attempt < max_retries - 1:
                wait = backoff_sec[attempt]
                print(f"[WARN] {type(e).__name__} => sleeping {wait}s before retry on {url} ...")
                time.sleep(wait)
                attempt += 1
            else:
                print(f"[ERROR] {type(e).__name__} after max retries => skipping {url}")
                return None
    return None


def get_listing_links(session, page_number):
    """
    Fetch the listing page for 'page_number' and collect up to 16 product-result links.
    If no 'product-result-1', we assume no results => stop pagination.
    """
    url = LISTING_URL_TEMPLATE.format(page=page_number)
    resp = robust_fetch(session, url)
    if not resp or not resp.ok:
        return []

    tree = html.fromstring(resp.text)
    # Check if product-result-1 exists. If not, no results => stop
    first_product = tree.xpath('//main[@id="main-content"]//a[@data-testid="product-result-1"]/@href')
    if not first_product:
        return []

    # Gather product-result-i links up to i=16
    links = []
    for i in range(1, 17):
        xp = f'//main[@id="main-content"]//a[@data-testid="product-result-{i}"]/@href'
        found = tree.xpath(xp)
        if found:
            links.extend(found)

    full_links = [urljoin(BASE_URL, ln) for ln in links]
    return full_links


def parse_detail_page(session, detail_url):
    """
    Fetch detail page and extract data fields into a dict.
    Return the dict or None if fetch fails.
    """
    data = dict.fromkeys(CSV_COLUMNS, None)
    data["URL"] = detail_url

    resp = robust_fetch(session, detail_url)
    if not resp or not resp.ok:
        print(f"[ERROR] Could not fetch detail page => skipping: {detail_url}")
        return None

    tree = html.fromstring(resp.text)

    def t(xp):
        return tree.xpath(xp)

    title = t('//h1[@class="h1-sm tablet:h1 text-trustful-1"]/text()')
    data["Title"] = title[0].strip() if title else None

    subtitle = t('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()')
    data["Subtitle"] = subtitle[0].strip() if subtitle else None

    fl_price = t('//div[@data-testid="price-block"]//h2/text()')
    data["Financial Lease Price"] = fl_price[0].strip() if fl_price else None

    fl_term = t('//div[@data-testid="price-block"]//p[contains(@class,"info-sm") and contains(text(),"mnd")]/text()')
    data["Financial Lease Term"] = fl_term[0].strip() if fl_term else None

    ad_num = t('//div[contains(@class,"p-sm") and contains(text(),"Advertentienummer")]/text()')
    if ad_num:
        data["Advertentienummer"] = ad_num[0].split(":", 1)[-1].strip()

    def spec(label):
        xp = f'//div[@class="text-p-sm text-grey-1" and normalize-space(text())="{label}"]/following-sibling::div/text()'
        v = t(xp)
        return v[0].strip() if v else None

    data["Merk"] = spec("Merk")
    data["Model"] = spec("Model")
    data["Bouwjaar"] = spec("Bouwjaar")
    data["Km stand"] = spec("Km stand")
    data["Transmissie"] = spec("Transmissie")
    data["Prijs"] = spec("Prijs")
    data["Brandstof"] = spec("Brandstof")
    data["Btw/marge"] = spec("Btw/marge")

    oa = t('//h2[contains(.,"Opties & Accessoires")]/following-sibling::ul/li/text()')
    if oa:
        cleaned = [x.strip() for x in oa if x.strip()]
        data["Opties & Accessoires"] = ", ".join(cleaned)

    addr = t('//div[@class="flex justify-between"]/div/p[@class="text-p-sm font-light text-black tablet:text-p"]/text()')
    data["Address"] = addr[0].strip() if addr else None

    imgs = t('//ul[@class="swiper-wrapper pb-10"]/li/img/@src')
    if imgs:
        joined = []
        for i in imgs:
            joined.append(urljoin(BASE_URL, i) if i.startswith("/") else i)
        data["Images"] = ",".join(joined)

    print(f"[DEBUG] Scraped data for {detail_url}: {data}")
    return data


def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    all_links = []
    page_number = 1

    print("[INFO] Gathering listing links from scratch (no more links.csv).")
    while True:
        print(f"\n=== Fetching listing page {page_number} ===")
        links = get_listing_links(session, page_number)
        if not links:
            print(f"No more links found on page {page_number}. Stopping pagination.")
            break
        print(f"Found {len(links)} links on page {page_number}")
        all_links.extend(links)
        page_number += 1
        time.sleep(2)  # small pause between listing pages

    print(f"[INFO] Total listing links found: {len(all_links)}")

    # Overwrite dtc_lease_results.csv from scratch
    # If you prefer to store in memory first, you can do so. We'll do row-by-row for big data sets.
    with open("dtc_lease_results.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        total_scraped = 0
        for idx, link in enumerate(all_links, start=1):
            print(f"\n[INFO] Scraping detail {idx}/{len(all_links)} => {link}")
            record = parse_detail_page(session, link)
            if record is None:
                print(f"[WARN] Skipping listing {link} due to fetch error.")
            else:
                writer.writerow(record)
                total_scraped += 1
                print(f"[INFO] Scraped so far: {total_scraped}")
            # Sleep after each detail fetch
            time.sleep(2)

    print(f"[DONE] Wrote {total_scraped} records to dtc_lease_results.csv.")


if __name__ == "__main__":
    main()
