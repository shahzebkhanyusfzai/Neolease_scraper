#!/usr/bin/env python3
"""
DTC-Lease scraper  –  10-slice collectors, batched inserts
• span-based pagination
• PARSE_CHUNK   = 2 000 URLs fetched & parsed at a time
• LISTING_CHUNK = 2 000 car rows per execute_values()
• IMAGE_CHUNK   = 10 000 image rows per execute_values()
• rows that violate DB constraints are truncated (or skipped) – scraper keeps going
"""

import math, re, time, gc, os, logging, concurrent.futures, requests
from itertools import chain
from urllib.parse import urljoin

import psycopg2, psycopg2.extras
from lxml import html

# ───────────────────────── CONFIG ─────────────────────────
BASE_URL      = "https://www.dtc-lease.nl"
HEADERS       = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
WORKERS       = 10

PARSE_CHUNK   = 2_000
LISTING_CHUNK = 2_000
IMAGE_CHUNK   = 10_000          # ≈50 MB for tuples

DB_DSN = (
    "dbname=neolease_db_kpz9 "
    "user=neolease_db_kpz9_user "
    "password=33H6QVFnAouvau72DlSjuKAMe5GdfviD "
    "host=dpg-d0f0ihh5pdvs73b6h3bg-a.oregon-postgres.render.com "
    "port=5432 sslmode=require"
)

# optional RAM meter (shows −1 MB if psutil absent)
try:
    import psutil
    mem_mb = lambda: psutil.Process(os.getpid()).memory_info().rss // 1_048_576
except ImportError:
    mem_mb = lambda: -1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)

# ───────────────────────── length guard ─────────────────────────
# bump these if you migrate columns to TEXT or larger VARCHAR
LIMIT = dict(title=160, subtitle=240, address=240, url=200)
def clip(val, field):
    lim = LIMIT.get(field)
    return val[:lim] if lim and isinstance(val, str) and len(val) > lim else val

# ───────────────────────── image extractor ─────────────────────────
IMG_XPATHS = [
    '//ul[contains(@class,"swiper-wrapper")]//img/@src',
    '//picture/source/@srcset',
    '//picture/img/@src',
]
def extract_images(tree):
    raw = list(chain.from_iterable(tree.xpath(xp) for xp in IMG_XPATHS))
    cleaned = []
    for r in raw:
        for seg in r.split(","):                # handle srcset
            u = seg.strip().split()[0]
            if not u:
                continue
            if u.startswith("//"):
                u = "https:" + u
            elif u.startswith("/"):
                u = urljoin(BASE_URL, u)
            cleaned.append(u)
    seen, out = set(), []
    for u in cleaned:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

# ───────────────────────── HTTP helpers ─────────────────────────
def http(url, sess, tries=3):
    for i in range(tries):
        try:
            r = sess.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            if r.status_code in (403, 429) and i < tries-1:
                time.sleep((i+1)*10); continue
            return r
        except (requests.Timeout, requests.ConnectionError):
            if i < tries-1: time.sleep((i+1)*10)
    return None

def total_results(tree):
    t = tree.xpath('//span[contains(text(),"Toon resultaten")]/text()')
    m = re.search(r'\((\d+)\)', t[0]) if t else None
    return int(m.group(1)) if m else None

def list_links(tree):
    links=[]
    for i in range(16):
        links += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL,u) for u in links]

# ───────────────────────── slice worker (collect URLs) ─────────────────────────
def slice_worker(idx, brands):
    sess=requests.Session(); urls=set()
    for b in brands:
        base=f"{b}?lease_type=financial&entity=business"
        r=http(base,sess); pages=cars=0
        if not r: continue
        tree=html.fromstring(r.text)
        L=list_links(tree); urls.update(L)
        pages+=1; cars+=len(L)
        max_p=math.ceil(total_results(tree)/16) if total_results(tree) else None
        p=1
        while True:
            p+=1
            if max_p and p>max_p: break
            r=http(f"{base}&page={p}",sess)
            if not r or 'product-result-1' not in r.text: break
            L=list_links(html.fromstring(r.text))
            if not L: break
            urls.update(L); cars+=len(L); pages+=1
            time.sleep(0.3)
        logging.info(f"[slice{idx}] {b} pages={pages} cars={cars}")
    logging.info(f"[slice{idx}] done total URLs={len(urls)}")
    return urls

def collect_links():
    sess=requests.Session()
    brands=[urljoin(BASE_URL,u) for u in
            html.fromstring(http(urljoin(BASE_URL,"/merken"),sess).text)
            .xpath('//main//ul/li/a/@href')]
    logging.info(f"[phase1] brand-model links = {len(brands)}")
    chunk=math.ceil(len(brands)/WORKERS)
    slices=[brands[i:i+chunk] for i in range(0,len(brands),chunk)]
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        sets=pool.map(lambda t:slice_worker(*t), ((i+1,s) for i,s in enumerate(slices)))
    links=set().union(*sets)
    logging.info(f"[phase1] GRAND TOTAL detail URLs = {len(links)}")
    return links

# ───────────────────────── detail scraper ─────────────────────────
def scrape_detail(url):
    sess=requests.Session(); r=http(url,sess)
    if not r: return None
    tree=html.fromstring(r.text); t=tree.xpath
    val = lambda xp:(t(xp) or [None])[0]
    spec= lambda lbl: val(f'//div[normalize-space(text())="{lbl}"]/following-sibling::div/text()')
    images=extract_images(tree)
    if not images:                            # skip totally imageless cars
        logging.warning(f"[no-img] {url}")
        return None
    return dict(
        url=url,
        title=val('//h1/text()'),
        subtitle=val('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()'),
        financial_lease_price=val('//div[@data-testid="price-block"]//h2/text()'),
        financial_lease_term=val('//div[@data-testid="price-block"]//p[contains(text(),"mnd")]/text()'),
        advertentienummer=val('//div[contains(text(),"Advertentienummer")]/text()'),
        merk=spec("Merk"), model=spec("Model"), bouwjaar=spec("Bouwjaar"),
        km_stand=spec("Km stand"), transmissie=spec("Transmissie"),
        prijs=spec("Prijs"), brandstof=spec("Brandstof"), btw_marge=spec("Btw/marge"),
        opties_accessoires=", ".join(x.strip() for x in t('//h2[contains(.,"Opties")]/following-sibling::ul/li/text()')) or None,
        address=val('//div[@class="flex justify-between"]/div/p/text()'),
        images=images
    )

# ───────────────────────── robust execute_values() ─────────────────────────
def execute_values_returning(cur, sql, rows, **kw):
    """
    Bulk insert with automatic salvage; always returns ids of rows that
    made it into the table.  Bad rows are skipped & returned.
    """
    inserted, bad = [], []
    cur.execute("SAVEPOINT bulk")
    try:
        psycopg2.extras.execute_values(cur, sql, rows, **kw)
        inserted = [row[0] for row in cur.fetchall()]
        cur.execute("RELEASE SAVEPOINT bulk")
        return inserted, bad
    except psycopg2.Error:
        cur.execute("ROLLBACK TO bulk")
        for row in rows:
            try:
                cur.execute(sql, (row,))
                inserted.append(cur.fetchone()[0])
            except psycopg2.Error:
                cur.connection.rollback()
                bad.append(row)
        return inserted, bad

# ───────────────────────── bulk insert ─────────────────────────
INS_LISTINGS = """
INSERT INTO car_listings (
 url,title,subtitle,financial_lease_price,financial_lease_term,
 advertentienummer,merk,model,bouwjaar,km_stand,
 transmissie,prijs,brandstof,btw_marge,opties_accessoires,address
) VALUES %s RETURNING id;
"""
INS_IMAGES = "INSERT INTO car_images (image_url,car_listing_id) VALUES %s"

def bulk_insert(cur, recs):
    skipped = []
    for i in range(0,len(recs),LISTING_CHUNK):
        chunk=recs[i:i+LISTING_CHUNK]
        listing_rows=[(
            clip(r["url"],"url"), clip(r["title"],"title"),
            clip(r["subtitle"],"subtitle"), r["financial_lease_price"],
            r["financial_lease_term"], r["advertentienummer"], r["merk"],
            r["model"], r["bouwjaar"], r["km_stand"], r["transmissie"],
            r["prijs"], r["brandstof"], r["btw_marge"],
            r["opties_accessoires"], clip(r["address"],"address")
        ) for r in chunk]

        ids, bad_rows = execute_values_returning(cur, INS_LISTINGS, listing_rows, page_size=500)
        skipped.extend(bad_rows)

        # build image tuples only for successfully inserted rows
        img_rows=[]
        for rec,lid in zip(chunk,ids):
            img_rows.extend((u,lid) for u in rec["images"])

        for j in range(0,len(img_rows),IMAGE_CHUNK):
            execute_values_returning(cur, INS_IMAGES, img_rows[j:j+IMAGE_CHUNK], page_size=1000)
    return skipped

# ───────────────────────── main ─────────────────────────
def main():
    logging.info("scraper start")
    links = collect_links()
    conn,cur=psycopg2.connect(DB_DSN),None
    try:
        cur=conn.cursor()
        cur.execute("SELECT url FROM car_listings;")
        existing={u for (u,) in cur.fetchall()}
        new_urls=list(links-existing)
        obsolete=list(existing-links)
        logging.info(f"new={len(new_urls)} obsolete={len(obsolete)}")

        # delete obsolete
        if obsolete:
            cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            ids=[i for (i,) in cur.fetchall()]
            if ids:
                cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
            cur.execute("DELETE FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            conn.commit()
            logging.info("obsolete deleted")

        # parse + insert
        if new_urls:
            logging.info("scraping details…")
            for i in range(0,len(new_urls),PARSE_CHUNK):
                batch=new_urls[i:i+PARSE_CHUNK]
                logging.info(f"[batch {i//PARSE_CHUNK+1}] urls={len(batch)}")
                with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
                    recs=[r for r in pool.map(scrape_detail,batch) if r]
                logging.info(f"[batch {i//PARSE_CHUNK+1}] parsed={len(recs)} mem={mem_mb()} MB → insert")
                skipped=bulk_insert(cur,recs); conn.commit()
                if skipped:
                    logging.warning(f"{len(skipped)} rows skipped – see log")
                del recs; gc.collect()

        logging.info("scraper finished OK")
    finally:
        if cur: cur.close()
        conn.close()

if __name__ == "__main__":
    main()
