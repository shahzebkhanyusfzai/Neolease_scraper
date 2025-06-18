#!/usr/bin/env python3
"""
DTC-Lease scraper – final one-shot version
──────────────────────────────────────────
 * Thread-pooled download (10 workers)
 * Robust JSON parser
 * Per-batch commit ⇒ crash-safe
 * Image <-> listing ID alignment guaranteed
"""

import os, re, time, gc, math, io, csv, json, logging, threading
import requests, psycopg2, psycopg2.extras
from lxml import html
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor

# ───── CONFIG ─────
BASE_URL = "https://www.dtc-lease.nl"
HEADERS  = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS  = 10

PARSE_CHUNK  = 2_000          # URLs scraped in parallel
IMAGE_CHUNK  = 10_000         # COPY this many images at once
DB_DSN = (
    "dbname=neolease_db_kpz9 "
    "user=neolease_db_kpz9_user "
    "password=33H6QVFnAouvau72DlSjuKAMe5GdfviD "
    "host=dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com "
    "port=5432 sslmode=require"
)

# ───── column lists ─────
CORE = [
    "url","title","subtitle","financial_lease_price","financial_lease_term",
    "advertentienummer","merk","model","bouwjaar","km_stand",
    "transmissie","prijs","brandstof","btw_marge",
    "opties_accessoires","address"
]
EXTRA = [
    "voertuigsoort","gebruikt_nieuw","inclusief_btw","inclusief_bpm","type",
    "inrichting","aantal_versnellingen","carrosserie","bekleding",
    "aantal_deuren","aantal_zitplaatsen","kleur_basis","bovag","nap",
    "vermogen_motor","cilinderinhoud","aantal_cilinders","wielbasis",
    "gewicht","topsnelheid","energielabel","gemiddeld_verbruik","tankinhoud"
]
ALL_FIELDS = CORE + EXTRA + ["images"]

INS_LISTINGS = (
    f"INSERT INTO car_listings ({', '.join(f for f in ALL_FIELDS if f!='images')}) "
    f"VALUES %s RETURNING id"
)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

# ───── helpers ─────
CLIP = dict(title=120, subtitle=120, address=240, url=200)
def clip(v, col):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, (list, dict)):
        v = json.dumps(v, ensure_ascii=False)
    v = str(v)
    lim = CLIP.get(col, 120)
    return v[:lim] if len(v) > lim else v

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

# ───── phase-1 : gather detail URLs ─────
def brand_links(sess):
    tree = html.fromstring(http(urljoin(BASE_URL, "/merken"), sess).text)
    return [urljoin(BASE_URL, u) for u in tree.xpath('//main//ul/li/a/@href')]

def page_url(b, p):
    pr = urlparse(b); qs = parse_qs(pr.query); qs["page"] = [str(p)]
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
            if not r: break
            lst = list_links(html.fromstring(r.text))
            if not lst: break
            urls.update(lst)
    logging.info("[slice%d] URLs=%d", idx, len(urls))
    return urls

def collect_links():
    sess = requests.Session()
    brands = brand_links(sess)
    step = math.ceil(len(brands) / WORKERS)
    slices = [brands[i:i+step] for i in range(0, len(brands), step)]
    with ThreadPoolExecutor(WORKERS) as pool:
        sets = pool.map(lambda t: slice_worker(*t),
                        ((i+1, s) for i, s in enumerate(slices)))
    return set().union(*sets)

# ───── phase-2 : scrape JSON → record ─────
def api_endpoint(bid, cid):
    return f"{BASE_URL}/_next/data/{bid}/voorraad/{cid}.json?id={cid}"

def pval(pd, k):
    fld = pd.get(k, {})
    return fld.get("value") or fld.get("name") or None

def json_to_record(url, js):
    inner = js.get("pageProps", {}).get("pageProps", {})
    if "product" not in inner:
        return None
    prod, pd = inner["product"], inner["product"].get("product_data", {})
    r = {k: None for k in ALL_FIELDS}
    r.update({
        "url": url,
        "title": f"{pval(pd,'merk')} {pval(pd,'model')}".strip(),
        "subtitle": pval(pd, "type"),
        "financial_lease_price": prod.get("from_price_business") or prod.get("from_price"),
        "financial_lease_term": "o.b.v. 72 mnd looptijd",
        "merk": pval(pd,"merk"), "model": pval(pd,"model"),
        "bouwjaar": pval(pd,"bouwjaar"), "km_stand": pval(pd,"km_stand"),
        "transmissie": pval(pd,"transmissie"), "prijs": pval(pd,"prijs"),
        "brandstof": pval(pd,"brandstof"), "btw_marge": pval(pd,"btw_marge"),
        "opties_accessoires": ", ".join(prod.get("accessoires", [])) or None,
        "address": prod.get("dealer", {}).get("Plaats_dealer"),
        "images": prod.get("afbeeldingen", []),
    })
    for k in EXTRA:
        r[k] = pval(pd, k)
    return r if r["images"] else None

def scrape_detail(tlocal, url, bid):
    sess = getattr(tlocal, "sess", None)
    if sess is None:
        sess = tlocal.sess = requests.Session()
    cid = url.rstrip("/").split("/")[-1]
    r = http(api_endpoint(bid, cid), sess)
    if not r: return None
    try:    return json_to_record(url, r.json())
    except ValueError: return None

# ───── DB helpers ─────
def copy_images(cur, rows):
    if not rows: return
    buf = io.StringIO()
    csv.writer(buf, delimiter='\t', lineterminator='\n').writerows(rows)
    buf.seek(0)
    cur.copy_from(buf, 'car_images', columns=('image_url','car_listing_id'))

def insert_listing_rows(cur, rows, page_size=200):
    """Return aligned id list (None for bad rows)."""
    try:
        psycopg2.extras.execute_values(cur, INS_LISTINGS, rows, page_size=page_size)
        return [r[0] for r in cur.fetchall()]
    except psycopg2.Error:
        cur.connection.rollback()
        ids = []
        for r in rows:
            try:
                cur.execute(INS_LISTINGS, (r,))
                ids.append(cur.fetchone()[0])
            except psycopg2.Error:
                cur.connection.rollback()
                ids.append(None)
        return ids

def bulk_insert(cur, recs):
    listing_rows = [tuple(clip(r[f],f) if f!="images" else None
                          for f in ALL_FIELDS if f!="images") for r in recs]

    ids = insert_listing_rows(cur, listing_rows)

    img_rows = []
    for rec, lid in zip(recs, ids):
        if lid is None:
            continue
        img_rows.extend((u, lid) for u in rec["images"])

    for start in range(0, len(img_rows), IMAGE_CHUNK):
        copy_images(cur, img_rows[start:start+IMAGE_CHUNK])

# ───── MAIN ─────
def main():
    logging.info("scraper start")
    build_id = get_build_id(requests.Session())
    links = collect_links()
    logging.info("detail URLs = %d", len(links))

    conn = psycopg2.connect(DB_DSN)
    cur  = conn.cursor()

    # URL diff
    cur.execute("SELECT url FROM car_listings;")
    existing = {u for (u,) in cur.fetchall()}
    new_urls = [u for u in links if u not in existing]
    obsolete = [u for u in existing if u not in links]
    logging.info("new=%d obsolete=%d", len(new_urls), len(obsolete))

    if obsolete:
        cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete,))
        ids = [i for (i,) in cur.fetchall()]
        if ids:
            cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
            cur.execute("DELETE FROM car_listings WHERE id = ANY(%s)", (ids,))
            conn.commit()
            logging.info("obsolete deleted")

    tlocal = threading.local()
    inserted_total = 0

    for off in range(0, len(new_urls), PARSE_CHUNK):
        batch = new_urls[off:off+PARSE_CHUNK]
        logging.info("[batch %d] urls=%d", off//PARSE_CHUNK+1, len(batch))

        with ThreadPoolExecutor(WORKERS) as pool:
            recs = [r for r in pool.map(lambda u: scrape_detail(tlocal,u,build_id), batch) if r]

        bulk_insert(cur, recs)
        conn.commit()
        inserted_total += len(recs)
        logging.info("committed, total inserted so far = %d", inserted_total)

        del recs
        gc.collect()

    logging.info("scraper finished, total new rows = %d", inserted_total)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
