#!/usr/bin/env python3
"""
DTC-Lease scraper – Render-ready, resilient version
────────────────────────────────────────────────────
* Multithread download
* COPY for car_images
* Universal clipping & type-safety
* Bulk-insert with per-row fallback
"""

import os, re, time, gc, math, io, csv, logging, threading, json
import requests, psycopg2, psycopg2.extras
from lxml import html
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor

# ───────── CONFIG ─────────
BASE_URL   = "https://www.dtc-lease.nl"
HEADERS    = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS    = 10

PARSE_CHUNK   = 2_000          # scrape this many URLs in parallel
COMMIT_EVERY  = 10_000         # commit every N inserted listings
IMAGE_CHUNK   = 10_000         # COPY this many images at once

DB_DSN = (
    "dbname=neolease_db_kpz9 "
    "user=neolease_db_kpz9_user "
    "password=33H6QVFnAouvau72DlSjuKAMe5GdfviD "
    "host=dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com "
    "port=5432 sslmode=require"
)

# ───────── column lists ─────────
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
    f"VALUES %s RETURNING id"
)

# ───────── logging ─────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)

# ───────── helpers ─────────
CLIP_LIMITS = dict(title=120, subtitle=120, address=240, url=200)
DEFAULT_CLIP = 120

def clip_val(val, col):
    """Guarantee DB-safe scalar within varchar limit."""
    if val is None:
        return None
    # lists / dicts -> JSON string
    if isinstance(val, (list, dict)):
        val = json.dumps(val, ensure_ascii=False)
    # everything else -> str or leave numeric
    if isinstance(val, (int, float, bool)):
        return val
    val = str(val)
    lim = CLIP_LIMITS.get(col, DEFAULT_CLIP)
    return val[:lim] if len(val) > lim else val

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

# ───────── phase-1 : collect all detail URLs ─────────
def brand_links(sess):
    tree = html.fromstring(http(urljoin(BASE_URL, "/merken"), sess).text)
    return [urljoin(BASE_URL, u) for u in tree.xpath('//main//ul/li/a/@href')]

def page_url(base, p):
    pr = urlparse(base); qs = parse_qs(pr.query); qs["page"] = [str(p)]
    return pr._replace(query=urlencode(qs, doseq=True)).geturl()

def list_links(tree):
    links = []
    for i in range(16):
        links += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL, u) for u in links]

def slice_worker(idx, brands):
    sess = requests.Session(); urls = set()
    for b in brands:
        page = 1
        while True:
            r = http(page_url(b, page), sess); page += 1
            if not r:
                break
            lst = list_links(html.fromstring(r.text))
            if not lst:
                break
            urls.update(lst)
    logging.info(f"[slice{idx}] URLs={len(urls)}")
    return urls

def collect_links(workers):
    sess = requests.Session()
    brands = brand_links(sess)
    step   = math.ceil(len(brands) / workers)
    slices = [brands[i:i+step] for i in range(0, len(brands), step)]
    with ThreadPoolExecutor(workers) as pool:
        sets = pool.map(lambda t: slice_worker(*t),
                        ((i+1, chunk) for i, chunk in enumerate(slices)))
    return set().union(*sets)

# ───────── phase-2 : scrape JSON → record ─────────
def api_endpoint(bid, cid):
    return f"{BASE_URL}/_next/data/{bid}/voorraad/{cid}.json?id={cid}"

def pval(pd, key):
    fld = pd.get(key, {})
    return fld.get("value") or fld.get("name") or None

def json_to_record(url, js):
    inner = js.get("pageProps", {}).get("pageProps", {})
    if "product" not in inner:
        return None
    prod, pd = inner["product"], inner["product"].get("product_data", {})
    rec = {k: None for k in ALL_FIELDS}
    rec.update({
        "url": url,
        "title": f"{pval(pd,'merk')} {pval(pd,'model')}".strip(),
        "subtitle": pval(pd, "type"),
        "financial_lease_price": prod.get("from_price_business") or prod.get("from_price"),
        "financial_lease_term": "o.b.v. 72 mnd looptijd",
        "merk": pval(pd, "merk"), "model": pval(pd, "model"),
        "bouwjaar": pval(pd, "bouwjaar"), "km_stand": pval(pd, "km_stand"),
        "transmissie": pval(pd, "transmissie"), "prijs": pval(pd, "prijs"),
        "brandstof": pval(pd, "brandstof"), "btw_marge": pval(pd, "btw_marge"),
        "opties_accessoires": ", ".join(prod.get("accessoires", [])) or None,
        "address": prod.get("dealer", {}).get("Plaats_dealer"),
        "images": prod.get("afbeeldingen", []),
    })
    for k in EXTRA:
        rec[k] = pval(pd, k)
    return rec if rec["images"] else None

def scrape_detail(tlocal, url, bid):
    sess = getattr(tlocal, "sess", None)
    if sess is None:
        sess = tlocal.sess = requests.Session()
    cid  = url.rstrip("/").split("/")[-1]
    r    = http(api_endpoint(bid, cid), sess)
    if not r:
        return None
    try:
        return json_to_record(url, r.json())
    except ValueError:
        return None

# ───────── DB helpers ─────────
def copy_images(cur, rows):
    if not rows:
        return
    buf = io.StringIO()
    csv.writer(buf, delimiter='\t', lineterminator='\n').writerows(rows)
    buf.seek(0)
    cur.copy_from(buf, 'car_images', columns=('image_url', 'car_listing_id'))

def safe_execute_values(cur, sql, rows, page_size=1000):
    """Try bulk; on failure fall back row-by-row, skipping bad rows."""
    try:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
        return len(rows), 0
    except psycopg2.Error as e:
        cur.connection.rollback()
        good, bad = 0, 0
        for r in rows:
            try:
                cur.execute(sql, (r,))
                good += 1
            except psycopg2.Error:
                cur.connection.rollback()
                bad += 1
        logging.warning(f"execute_values fallback: {bad} rows skipped ({e.pgerror.strip()})")
        return good, bad

def bulk_insert(cur, recs):
    listing_rows, img_rows = [], []
    for r in recs:
        listing_rows.append(tuple(
            clip_val(r[f], f) if f != "images" else None
            for f in ALL_FIELDS if f != "images"
        ))
    good, _ = safe_execute_values(cur, INS_LISTINGS, listing_rows, page_size=1000)
    ids = [row[0] for row in cur.fetchall()]
    for imgs, lid in zip((r["images"] for r in recs), ids):
        img_rows.extend((u, lid) for u in imgs)
    for start in range(0, len(img_rows), IMAGE_CHUNK):
        copy_images(cur, img_rows[start:start + IMAGE_CHUNK])
    return good

# ───────── MAIN ─────────
def main():
    logging.info("scraper start")
    build_id = get_build_id(requests.Session())
    links    = collect_links(WORKERS)
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
            logging.info("nothing new – done")
            return

        tlocal, inserted = threading.local(), 0
        for off in range(0, len(new_urls), PARSE_CHUNK):
            batch = new_urls[off:off + PARSE_CHUNK]
            logging.info(f"[batch {off//PARSE_CHUNK + 1}] urls={len(batch)}")

            with ThreadPoolExecutor(WORKERS) as pool:
                recs = [r for r in pool.map(lambda u: scrape_detail(tlocal, u, build_id), batch) if r]

            inserted += bulk_insert(cur, recs)

            if inserted >= COMMIT_EVERY:
                conn.commit(); inserted = 0; logging.info("committed")

            del recs; gc.collect()

        if inserted:
            conn.commit()
        logging.info("scraper finished OK")
    finally:
        if cur: cur.close()
        conn.close()

if __name__ == "__main__":
    main()
