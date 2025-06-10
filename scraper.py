#!/usr/bin/env python3
"""
DTC-Lease scraper – Render version – JSON / _next/data endpoint
────────────────────────────────────────────────────────────────
* Same logic as before (multithread, diff vs. DB, delete obsolete)
* Faster DB phase via COPY for car_images
"""

import os, re, time, gc, math, logging, threading, io, csv
import requests, psycopg2, psycopg2.extras
from lxml import html
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor

# ───────── CONFIG ─────────
BASE_URL   = "https://www.dtc-lease.nl"
HEADERS    = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS    = 10

PARSE_CHUNK   = 2_000   # scrape this many URLs in parallel
LISTING_CHUNK = 2_000   # insert this many listings per bulk
COMMIT_EVERY  = 10_000  # commit after this many new listings
IMAGE_CHUNK   = 10_000  # COPY this many images at once

DB_DSN = (
    "dbname=neolease_db_kpz9 "
    "user=neolease_db_kpz9_user "
    "password=33H6QVFnAouvau72DlSjuKAMe5GdfviD "
    "host=dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com "
    "port=5432 sslmode=require"
)

# ───────── columns & SQL ─────────
CORE = [
    "url","title","subtitle","financial_lease_price","financial_lease_term",
    "advertentienummer","merk","model","bouwjaar","km_stand",
    "transmissie","prijs","brandstof","btw_marge",
    "opties_accessoires","address"
]
EXTRA = [
    "voertuigsoort","gebruikt_nieuw","inclusief_btw","inclusief_bpm",
    "type","inrichting","aantal_versnellingen","carrosserie","bekleding",
    "aantal_deuren","aantal_zitplaatsen","kleur_basis","bovag","nap",
    "vermogen_motor","cilinderinhoud","aantal_cilinders","wielbasis",
    "gewicht","topsnelheid","energielabel","gemiddeld_verbruik",
    "tankinhoud"
]
ALL_FIELDS = CORE + EXTRA + ["images"]

INS_LISTINGS = (
    f"INSERT INTO car_listings ({', '.join(f for f in ALL_FIELDS if f!='images')}) "
    f"VALUES %s RETURNING id;"
)

# ───────── helpers ─────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
)

LIMIT = dict(title=160, subtitle=240, address=240, url=200)
clip  = lambda v,f: (v[:LIMIT[f]] if f in LIMIT and v and len(v)>LIMIT[f] else v)

def http(url, sess, tries=4, backoff=8):
    for i in range(tries):
        try:
            r = sess.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r
        except requests.exceptions.RequestException:
            pass
        time.sleep(backoff * (i + 1))
    return None

def get_build_id(sess):
    m = re.search(r'"buildId":"([^"]+)"', http(BASE_URL, sess).text)
    if not m:
        raise RuntimeError("buildId not found")
    return m.group(1)

# ───────── phase-1 : harvest all detail URLs ─────────
def brand_links(sess):
    page = http(urljoin(BASE_URL, "/merken"), sess)
    tree = html.fromstring(page.text)
    return [urljoin(BASE_URL, u) for u in tree.xpath('//main//ul/li/a/@href')]

def page_url(b, p):
    pr = urlparse(b)
    qs = parse_qs(pr.query); qs["page"] = [str(p)]
    return pr._replace(query=urlencode(qs, doseq=True)).geturl()

def list_links(tree):
    out = []
    for i in range(16):
        out += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL, u) for u in out]

def slice_worker(idx, brands):
    sess = requests.Session(); urls = set()
    for b in brands:
        p = 1
        while True:
            r = http(page_url(b, p), sess); p += 1
            if not r:
                break
            L = list_links(html.fromstring(r.text))
            if not L:
                break
            urls.update(L)
    logging.info(f"[slice{idx}] URLs={len(urls)}")
    return urls

def collect_links(build_id):
    sess = requests.Session()
    brands = brand_links(sess)
    chunk  = math.ceil(len(brands) / WORKERS)
    slices = [brands[i:i+chunk] for i in range(0, len(brands), chunk)]
    with ThreadPoolExecutor(WORKERS) as pool:
        sets = pool.map(lambda t: slice_worker(*t), ((i+1,s) for i,s in enumerate(slices)))
    return set().union(*sets)

# ───────── phase-2 : detail JSON → record ─────────
def api_endpoint(build_id, car_id):
    return f"{BASE_URL}/_next/data/{build_id}/voorraad/{car_id}.json?id={car_id}"

def pval(pd, key):
    fld = pd.get(key, {})
    return fld.get("value") or fld.get("name") or None

def json_to_record(detail_url, data):
    inner = data.get("pageProps", {}).get("pageProps", {})
    if "product" not in inner:
        return None
    prod, pd = inner["product"], inner["product"].get("product_data", {})
    r = dict.fromkeys(ALL_FIELDS)
    r.update({
        "url": detail_url,
        "title": f"{pval(pd,'merk')} {pval(pd,'model')}".strip(),
        "subtitle": pval(pd, "type"),
        "financial_lease_price": prod.get("from_price_business") or prod.get("from_price"),
        "financial_lease_term": "o.b.v. 72 mnd looptijd",
        "merk": pval(pd, "merk"),     "model": pval(pd, "model"),
        "bouwjaar": pval(pd, "bouwjaar"), "km_stand": pval(pd, "km_stand"),
        "transmissie": pval(pd, "transmissie"), "prijs": pval(pd, "prijs"),
        "brandstof": pval(pd, "brandstof"),     "btw_marge": pval(pd, "btw_marge"),
        "opties_accessoires": ", ".join(prod.get("accessoires", [])) or None,
        "address": prod.get("dealer", {}).get("Plaats_dealer"),
        "images": prod.get("afbeeldingen", []),
    })
    for k in EXTRA:
        r[k] = pval(pd, k)
    return r if r["images"] else None

def scrape_detail_api(thread_local, url, build_id):
    # one Session per thread (created lazily)
    sess = getattr(thread_local, "sess", None)
    if sess is None:
        sess = thread_local.sess = requests.Session()
    car_id = url.rstrip("/").split("/")[-1]
    r = http(api_endpoint(build_id, car_id), sess)
    if not r:
        return None
    try:
        return json_to_record(url, r.json())
    except ValueError:
        return None

# ───────── DB helpers ─────────
def copy_images(cur, img_rows):
    """Bulk-load image urls with COPY (much faster than many INSERTs)."""
    if not img_rows:
        return
    buf = io.StringIO()
    w   = csv.writer(buf, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    w.writerows(img_rows)
    buf.seek(0)
    cur.copy_from(buf, 'car_images', columns=('image_url', 'car_listing_id'))

def bulk_insert(cur, recs):
    skipped, listing_buf, img_buf = [], [], []

    for r in recs:
        listing_buf.append(tuple(clip(r[f],f) if f!="images" else None
                                 for f in ALL_FIELDS if f!="images"))
        img_buf.append(r["images"])

    # insert listings
    psycopg2.extras.execute_values(cur, INS_LISTINGS, listing_buf, page_size=1000)
    ids = [row[0] for row in cur.fetchall()]  # ids align with listing_buf order

    # flatten images & COPY
    img_rows = [(u, lid)
                for imgs, lid in zip(img_buf, ids)
                for u in imgs]
    for i in range(0, len(img_rows), IMAGE_CHUNK):
        copy_images(cur, img_rows[i:i+IMAGE_CHUNK])

    return skipped   # left for interface compatibility

# ───────── MAIN ─────────
def main():
    logging.info("scraper start")
    build_id = get_build_id(requests.Session())
    logging.info(f"buildId={build_id}")

    links = collect_links(build_id)
    logging.info(f"detail URLs = {len(links)}")

    conn, cur = psycopg2.connect(DB_DSN), None
    try:
        cur = conn.cursor()
        cur.execute("SELECT url FROM car_listings;")
        existing = {u for (u,) in cur.fetchall()}

        new_urls = [u for u in links if u not in existing]
        obsolete = [u for u in existing if u not in links]
        logging.info(f"new={len(new_urls)} obsolete={len(obsolete)}")

        if obsolete:
            cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            ids = [i for (i,) in cur.fetchall()]
            if ids:
                cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
                cur.execute("DELETE FROM car_listings WHERE id = ANY(%s)", (ids,))
                conn.commit()
                logging.info("obsolete deleted")

        if not new_urls:
            logging.info("nothing new – done"); return

        # scrape & insert in batches
        thread_local = threading.local()
        total_inserted = 0

        for start in range(0, len(new_urls), PARSE_CHUNK):
            batch = new_urls[start : start + PARSE_CHUNK]
            logging.info(f"[batch {start//PARSE_CHUNK + 1}] urls={len(batch)}")

            with ThreadPoolExecutor(WORKERS) as pool:
                recs = [r for r in pool.map(
                            lambda u: scrape_detail_api(thread_local, u, build_id), batch)
                        if r]

            import random; random.shuffle(recs)
            logging.info(f"[batch {start//PARSE_CHUNK + 1}] shuffled={len(recs)}")

            bulk_insert(cur, recs)
            total_inserted += len(recs)

            # commit every COMMIT_EVERY listings
            if total_inserted >= COMMIT_EVERY:
                conn.commit(); total_inserted = 0
                logging.info("committed")

            del recs; gc.collect()

        if total_inserted:
            conn.commit()
        logging.info("scraper finished OK")

    finally:
        if cur: cur.close()
        conn.close()

if __name__ == "__main__":
    main()
