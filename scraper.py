#!/usr/bin/env python3
"""
DTC-Lease scraper – Render version – JSON / _next/data endpoint
────────────────────────────────────────────────────────────────
* Keeps the full batching / multithread / DB-insert logic you already use
* Replaces the old HTML parser with a pure-API parser
* Adds all new spec columns that live in product_data
"""

import os, re, time, gc, math, logging, concurrent.futures, requests
from itertools import chain
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import psycopg2, psycopg2.extras
from lxml import html

# ───────────────────────── CONFIG ─────────────────────────
BASE_URL      = "https://www.dtc-lease.nl"
HEADERS       = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS       = 10

PARSE_CHUNK   = 2_000
LISTING_CHUNK = 2_000
IMAGE_CHUNK   = 10_000

DB_DSN = (
    "dbname=neolease_db_kpz9 "
    "user=neolease_db_kpz9_user "
    "password=33H6QVFnAouvau72DlSjuKAMe5GdfviD "
    "host=dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com "
    "port=5432 sslmode=require"
)

# ── fields ────────────────────────────────────────────────
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

# ── DB insert templates  (order! must match ALL_FIELDS minus images) ──
INS_LISTINGS = f"""
INSERT INTO car_listings (
 {", ".join(f for f in ALL_FIELDS if f!="images")}
) VALUES %s RETURNING id;
"""
INS_IMAGES   = "INSERT INTO car_images (image_url,car_listing_id) VALUES %s"

# optional memory meter
try:
    import psutil
    mem_mb=lambda:psutil.Process(os.getpid()).memory_info().rss//1_048_576
except ImportError:
    mem_mb=lambda:-1

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)

# ───────────────────────── small helpers ─────────────────────────
LIMIT = dict(title=160, subtitle=240, address=240, url=200)
def clip(v,f): lim=LIMIT.get(f); return v[:lim] if lim and v and len(v)>lim else v

def http(url,sess,tries=4,backoff=8):
    for i in range(tries):
        try:
            r=sess.get(url,headers=HEADERS,timeout=20)
            if r.status_code==200: return r
        except requests.exceptions.RequestException:
            pass
        time.sleep(backoff*(i+1))
    return None

# ───────────────────────── discover buildId once ─────────────────────────
def get_build_id(sess):
    r=http(BASE_URL,sess)
    m=re.search(r'"buildId":"([^"]+)"',r.text)
    if not m: raise RuntimeError("buildId not found")
    return m.group(1)

# ───────────────────────── phase-1 : URL harvest ─────────────────────────
def brand_links(sess):
    r=http(urljoin(BASE_URL,"/merken"),sess)
    t=html.fromstring(r.text)
    return [urljoin(BASE_URL,u) for u in t.xpath('//main//ul/li/a/@href')]

def page_url(b,p):
    parsed=urlparse(b); qs=parse_qs(parsed.query); qs["page"]=[str(p)]
    return parsed._replace(query=urlencode(qs,doseq=True)).geturl()

def list_links(tree):
    out=[]
    for i in range(16):
        out+=tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL,u) for u in out]

def slice_worker(idx,brands):
    sess=requests.Session(); urls=set()
    for b in brands:
        p=1
        while True:
            r=http(page_url(b,p),sess); p+=1
            if not r: break
            L=list_links(html.fromstring(r.text))
            if not L: break
            urls.update(L)
    logging.info(f"[slice{idx}] URLs={len(urls)}")
    return urls

def collect_links():
    sess=requests.Session()
    brands=brand_links(sess)
    chunk=math.ceil(len(brands)/WORKERS)
    slices=[brands[i:i+chunk] for i in range(0,len(brands),chunk)]
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        sets=pool.map(lambda t:slice_worker(*t),((i+1,s) for i,s in enumerate(slices)))
    return set().union(*sets)

# ───────────────────────── phase-2 : detail JSON → record ─────────────────
def api_endpoint(build_id,car_id):
    return f"{BASE_URL}/_next/data/{build_id}/voorraad/{car_id}.json?id={car_id}"

def pval(pd,key):
    fld=pd.get(key,{}); return fld.get("value") or fld.get("name") or None

def json_to_record(detail_url,data):
    inner=data.get("pageProps",{}).get("pageProps",{})
    if "product" not in inner: return None
    prod=inner["product"]; pd=prod.get("product_data",{})
    rec=dict.fromkeys(ALL_FIELDS)
    rec.update({
        "url":detail_url,
        "title":f"{pval(pd,'merk')} {pval(pd,'model')}".strip(),
        "subtitle":pval(pd,"type"),
        "financial_lease_price":prod.get("from_price_business") or prod.get("from_price"),
        "financial_lease_term":"o.b.v. 72 mnd looptijd",
        "merk":pval(pd,"merk"), "model":pval(pd,"model"),
        "bouwjaar":pval(pd,"bouwjaar"), "km_stand":pval(pd,"km_stand"),
        "transmissie":pval(pd,"transmissie"), "prijs":pval(pd,"prijs"),
        "brandstof":pval(pd,"brandstof"), "btw_marge":pval(pd,"btw_marge"),
        "opties_accessoires":", ".join(prod.get("accessoires",[])) or None,
        "address":prod.get("dealer",{}).get("Plaats_dealer"),
        "images":prod.get("afbeeldingen",[])
    })
    for k in EXTRA:
        rec[k]=pval(pd,k)
    return rec if rec["images"] else None          # keep same “skip imageless” rule

def scrape_detail_api(url,build_id):
    sess=requests.Session(); car_id=url.rstrip("/").split("/")[-1]
    r=http(api_endpoint(build_id,car_id),sess)
    if not r: return None
    try:
        data=r.json()
    except ValueError:
        return None
    return json_to_record(url,data)

# ───────────────────────── DB helpers (unchanged) ─────────────────────────
def execute_values_returning(cur,sql,rows,**kw):
    cur.execute("SAVEPOINT bulk")
    try:
        psycopg2.extras.execute_values(cur,sql,rows,**kw)
        ids=[row[0] for row in cur.fetchall()]
        cur.execute("RELEASE SAVEPOINT bulk"); return ids,[]
    except psycopg2.Error:
        cur.execute("ROLLBACK TO bulk"); good,bad=[],[]
        for r in rows:
            try: cur.execute(sql,(r,)); good.append(cur.fetchone()[0])
            except psycopg2.Error: cur.connection.rollback(); bad.append(r)
        return good,bad

def bulk_insert(cur,recs):
    skipped=[]
    for i in range(0,len(recs),LISTING_CHUNK):
        chunk=recs[i:i+LISTING_CHUNK]
        listing_rows=[tuple(
            clip(r[f],f) if f!="images" else None for f in ALL_FIELDS if f!="images"
        ) for r in chunk]

        ids,bad=execute_values_returning(cur,INS_LISTINGS,listing_rows,page_size=500)
        skipped.extend(bad)

        img_rows=[]
        for rec,lid in zip(chunk,ids):
            img_rows.extend((u,lid) for u in rec["images"])

        for j in range(0,len(img_rows),IMAGE_CHUNK):
            execute_values_returning(cur,INS_IMAGES,img_rows[j:j+IMAGE_CHUNK],page_size=1000)
    return skipped

# ───────────────────────── MAIN ─────────────────────────
def main():
    logging.info("scraper start")
    build_id = get_build_id(requests.Session())
    logging.info(f"buildId={build_id}")

    links = collect_links()
    logging.info(f"detail URLs = {len(links)}")

    conn = psycopg2.connect(DB_DSN)
    cur  = None
    try:
        cur = conn.cursor()

        # Fetch existing URLs so we can diff
        cur.execute("SELECT url FROM car_listings;")
        existing = {u for (u,) in cur.fetchall()}

        new_urls   = [u for u in links if u not in existing]
        obsolete   = [u for u in existing if u not in links]
        logging.info(f"new={len(new_urls)} obsolete={len(obsolete)}")

        # Delete any listings (and images) that are no longer on the site
        if obsolete:
            cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            ids = [i for (i,) in cur.fetchall()]
            if ids:
                cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
                cur.execute("DELETE FROM car_listings WHERE id = ANY(%s)", (ids,))
                conn.commit()
                logging.info("obsolete deleted")

        # Process newly found URLs in batches
        if new_urls:
            logging.info("scraping details…")
            for batch_start in range(0, len(new_urls), PARSE_CHUNK):
                batch = new_urls[batch_start : batch_start + PARSE_CHUNK]
                logging.info(f"[batch {batch_start//PARSE_CHUNK + 1}] urls={len(batch)}")

                # Scrape each detail page in parallel
                with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
                    recs = [
                        r for r in pool.map(lambda u: scrape_detail_api(u, build_id), batch)
                        if r
                    ]

                # ── Shuffle this batch before inserting ──
                import random
                random.shuffle(recs)
                logging.info(f"[batch {batch_start//PARSE_CHUNK + 1}] shuffled={len(recs)} records")

                # Insert listings + images
                logging.info(f"[batch {batch_start//PARSE_CHUNK + 1}] inserting… mem={mem_mb()} MB")
                skipped = bulk_insert(cur, recs)
                conn.commit()
                if skipped:
                    logging.warning(f"{len(skipped)} rows skipped")

                # Clean up for the next batch
                del recs
                gc.collect()

        logging.info("scraper finished OK")

    finally:
        if cur:
            cur.close()
        conn.close()


if __name__ == "__main__":
    main()
