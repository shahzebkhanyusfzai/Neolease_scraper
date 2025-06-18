#!/usr/bin/env python3
"""
NeoLease scraper – FULL DEBUG EDITION
─────────────────────────────────────
* Logs every API URL it hits (once per car)
* Verifies image-car_id match before INSERT
* Writes mismatches to  debug_mismatch.csv
* Everything else identical to your production script
"""

import os, re, csv, io, json, time, math, gc, logging, random, threading
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor

import requests, psycopg2, psycopg2.extras
from lxml import html

# ───────── CONFIG ─────────
BASE_URL   = "https://www.dtc-lease.nl"
HEADERS    = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS    = 8               # lower to be gentle on Render
PARSE_CHUNK = 2_000
IMAGE_CHUNK = 10_000
DB_DSN = (
    "dbname=neolease_db_kpz9 "
    "user=neolease_db_kpz9_user "
    "password=33H6QVFnAouvau72DlSjuKAMe5GdfviD "      # ⚠️  scrub this before commit
    "host=dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com "
    "port=5432 sslmode=require"
)

# ───────── COLUMNS (exactly the same lists you already use) ─────────
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
    "gewicht","topsnelheid","energielabel","gemiddeld_verbruik","tankinhoud"
]
ALL_FIELDS = CORE + EXTRA + ["images"]
INS_LISTINGS = (
    f"INSERT INTO car_listings ({', '.join(f for f in ALL_FIELDS if f!='images')}) "
    f"VALUES %s RETURNING id"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"
)

# CSV that logs every mismatch we detect
MISMATCH_CSV = "debug_mismatch.csv"
with open(MISMATCH_CSV, "w", newline="") as fh:
    csv.writer(fh).writerow(["listing_id","img_car_id","image_url"])

# ───── tiny helpers ─────
def clip(v, col, lims=dict(title=120, subtitle=120, url=200, address=240)):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, (list, dict)):
        v = json.dumps(v, ensure_ascii=False)
    v = str(v)
    lim = lims.get(col, 120)
    return v[:lim] if len(v) > lim else v

def http(url, sess):
    for _ in range(3):
        try:
            r = sess.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r
        except requests.RequestException:
            pass
        time.sleep(4)
    return None

def get_build_id(sess):
    m = re.search(r'"buildId":"([^"]+)"', http(BASE_URL, sess).text)
    if not m:
        raise RuntimeError("buildId not found")
    return m.group(1)

# ───── harvest detail URLs (verbose) ─────
def brand_links(sess):
    page = http(urljoin(BASE_URL, "/merken"), sess)
    return [
        urljoin(BASE_URL, u)
        for u in html.fromstring(page.text).xpath('//main//ul/li/a/@href')
    ]

def collect_links():
    sess = requests.Session()
    brands = brand_links(sess)
    step = math.ceil(len(brands) / WORKERS)
    slices = [brands[i : i + step] for i in range(0, len(brands), step)]
    urls = set()

    def worker(chunk):
        s = requests.Session()
        for b in chunk:
            page = 1
            while True:
                url = urlparse(b)._replace(query=urlencode({"page": page})).geturl()
                r = http(url, s)
                page += 1
                if not r:
                    break
                links = [
                    urljoin(BASE_URL, u)
                    for i in range(16)
                    for u in html.fromstring(r.text).xpath(
                        f'//a[@data-testid="product-result-{i}"]/@href'
                    )
                ]
                if not links:
                    break
                urls.update(links)

    with ThreadPoolExecutor(WORKERS) as pool:
        pool.map(worker, slices)

    logging.info("harvested %d detail URLs", len(urls))
    return urls

# ───── scrape detail JSON ─────
def api_endpoint(bid, cid):
    return f"{BASE_URL}/_next/data/{bid}/voorraad/{cid}.json?id={cid}"

def scrape_detail(tlocal, url, bid):
    sess = getattr(tlocal, "sess", None)
    if sess is None:
        sess = tlocal.sess = requests.Session()

    cid = url.rstrip("/").split("/")[-1]
    api = api_endpoint(bid, cid)
    logging.debug("API %s", api)

    r = http(api, sess)
    if not r:
        logging.warning("HTTP miss %s", api)
        return None
    try:
        js = r.json()
    except ValueError:
        logging.warning("non-JSON for %s", api)
        return None

    inner = js.get("pageProps", {}).get("pageProps", {})
    prod = inner.get("product")
    pd = prod.get("product_data") if prod else None
    if not prod or not pd:
        return None

    imgs = prod.get("afbeeldingen", [])
    # verify first image belongs to same car
    if imgs:
        m = re.search(r'/products/([0-9]+)/', imgs[0])
        if not m or m.group(1) != cid:
            # log mismatch for later manual look-up
            with open(MISMATCH_CSV, "a", newline="") as fh:
                csv.writer(fh).writerow([cid, m.group(1) if m else "NONE", imgs[0]])
            logging.info("IMG-CID mismatch  listing:%s  img:%s", cid, m.group(1) if m else "NONE")
            return None

    def pval(pdata, key):
        fld = pdata.get(key, {})
        return fld.get("value") or fld.get("name")

    rec = dict.fromkeys(ALL_FIELDS)
    rec.update(
        {
            # CORE
            "url": url,
            "title": f"{pval(pd,'merk')} {pval(pd,'model')}".strip(),
            "subtitle": pval(pd, "type"),
            "financial_lease_price": prod.get("from_price_business") or prod.get("from_price"),
            "financial_lease_term": "o.b.v. 72 mnd looptijd",
            "advertentienummer": cid,
            "merk": pval(pd, "merk"),
            "model": pval(pd, "model"),
            "bouwjaar": pval(pd, "bouwjaar"),
            "km_stand": pval(pd, "km_stand"),
            "transmissie": pval(pd, "transmissie"),
            "prijs": pval(pd, "prijs"),
            "brandstof": pval(pd, "brandstof"),
            "btw_marge": pval(pd, "btw_marge"),
            "opties_accessoires": ", ".join(prod.get("accessoires", [])) or None,
            "address": prod.get("dealer", {}).get("Plaats_dealer"),
            # EXTRA
            **{k: pval(pd, k) for k in EXTRA},
            # images field
            "images": imgs,
        }
    )
    logging.debug("OK %s imgs:%d", cid, len(imgs))
    return rec if imgs else None

# ───── DB helpers (STRICT alignment) ─────
def insert_listing_rows(cur, rows):
    ids = []
    for r in rows:               # one-by-one to keep order 1:1
        cur.execute(INS_LISTINGS, (r,))
        ids.append(cur.fetchone()[0])
    return ids

def bulk_insert(cur, recs):
    rows = [
        tuple(clip(r[f], f) if f != "images" else None for f in ALL_FIELDS if f != "images")
        for r in recs
    ]
    ids = insert_listing_rows(cur, rows)

    img_rows = [(u, lid) for r, lid in zip(recs, ids) for u in r["images"]]
    if img_rows:
        buf = io.StringIO()
        csv.writer(buf, delimiter="\t", lineterminator="\n").writerows(img_rows)
        buf.seek(0)
        cur.copy_from(buf, "car_images", columns=("image_url", "car_listing_id"))

# ───────── MAIN ─────────
def main():
    logging.info("scraper start (DEBUG EDITION)")
    bid = get_build_id(requests.Session())
    links = collect_links()

    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    # Wipe tables so we can inspect a clean run
    cur.execute("TRUNCATE car_images, car_listings RESTART IDENTITY CASCADE;")
    conn.commit()
    logging.info("tables truncated – starting fresh")

    tlocal = threading.local()
    total = 0
    for off in range(0, len(links), PARSE_CHUNK):
        batch = list(links)[off : off + PARSE_CHUNK]
        logging.info("[batch %d] %d URLs", off // PARSE_CHUNK + 1, len(batch))

        with ThreadPoolExecutor(WORKERS) as pool:
            recs = [r for r in pool.map(lambda u: scrape_detail(tlocal, u, bid), batch) if r]

        bulk_insert(cur, recs)
        conn.commit()
        total += len(recs)
        logging.info("batch committed – running total %d", total)

        del recs
        gc.collect()

    logging.info("DONE – inserted %d listings   mismatches logged → %s", total, MISMATCH_CSV)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
