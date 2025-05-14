#!/usr/bin/env python3
"""
DTC-Lease scraper  –  10-slice collectors, batched inserts.
• span-based pagination
• PARSE_CHUNK   = 2 000 URLs fetched & parsed at a time
• LISTING_CHUNK = 2 000 rows pushed into Postgres per execute_values()
• IMAGE_CHUNK   = 10 000 image rows per execute_values()
• If *any* record fails to insert it is skipped and logged – scraper keeps going.
• No server-side image 404 checking (handled on front-end).
"""

import math, re, time, gc, os, logging, concurrent.futures, requests
import psycopg2, psycopg2.extras, psycopg2.errors
from urllib.parse import urljoin
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

# optional RAM meter (shows -1 MB if psutil missing)
try:
    import psutil
    mem_mb = lambda: psutil.Process(os.getpid()).memory_info().rss // 1_048_576
except ImportError:
    mem_mb = lambda: -1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S"
)


# ── helper (place it near the top, right after the imports) ───────────────
from itertools import chain
IMG_XPATHS = [
    # any <img> in the classical gallery
    '//ul[contains(@class,"swiper-wrapper")]//img/@src',
    # <picture> variants
    '//picture/source/@srcset',      # srcset contains 1–3 URLs
    '//picture/img/@src',
]
def extract_images(tree):
    """return deduped list of absolute image URLs, handles srcset"""
    raw = list(chain.from_iterable(tree.xpath(xp) for xp in IMG_XPATHS))
    cleaned = []
    for r in raw:
        # split comma-separated srcset and keep only the URL part
        for seg in r.split(","):
            u = seg.strip().split()[0]
            if not u:
                continue
            if u.startswith("//"):
                u = "https:" + u
            elif u.startswith("/"):
                u = urljoin(BASE_URL, u)
            cleaned.append(u)
    # de-duplicate while preserving order
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
    txt = tree.xpath('//span[contains(text(),"Toon resultaten")]/text()')
    if txt and (m := re.search(r'\((\d+)\)', txt[0])):
        return int(m.group(1))
    return None

def list_links(tree):
    out=[]
    for i in range(16):
        out += tree.xpath(f'//a[@data-testid="product-result-{i}"]/@href')
    return [urljoin(BASE_URL,u) for u in out]

# ───────────────────────── SLICE worker – collects URLs ─────────────────────────
def slice_worker(idx, brands):
    sess = requests.Session()
    urls = set()

    for b in brands:
        base=f"{b}?lease_type=financial&entity=business"
        r=http(base,sess); pages=cars=0
        if not r: continue

        tree=html.fromstring(r.text)
        L=list_links(tree); urls.update(L)
        pages+=1; cars+=len(L)
        max_p = math.ceil(total_results(tree)/16) if total_results(tree) else None
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

        logging.info(f"[slice{idx}] {b}  pages={pages}  cars={cars}")

    logging.info(f"[slice{idx}] finished  total URLs={len(urls)}")
    return urls

def collect_links():
    sess=requests.Session()
    brand_urls=[urljoin(BASE_URL,u) for u in
                html.fromstring(http(urljoin(BASE_URL,"/merken"),sess).text)
                .xpath('//main//ul/li/a/@href')]
    logging.info(f"[phase1] brand-model links = {len(brand_urls)}")

    chunk  = math.ceil(len(brand_urls)/WORKERS)
    slices = [brand_urls[i:i+chunk] for i in range(0,len(brand_urls),chunk)]

    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        sets = pool.map(lambda t:slice_worker(*t), ((i+1,s) for i,s in enumerate(slices)))
    links = set().union(*sets)
    logging.info(f"[phase1] GRAND TOTAL detail URLs = {len(links)}")
    return links

# ───────────────────────── DETAIL scraper ─────────────────────────
def scrape_detail(url):
    sess = requests.Session(); r = http(url,sess)
    if not r: return None

    tree=html.fromstring(r.text); t=tree.xpath
    val = lambda x:(t(x) or [None])[0]
    spec= lambda lbl: val(f'//div[normalize-space(text())="{lbl}"]/following-sibling::div/text()')

    return dict(
        url=url,
        title=val('//h1/text()'),
        subtitle=val('//p[@class="type-auto-sm tablet:type-auto-m text-trustful-1"]/text()'),
        financial_lease_price=val('//div[@data-testid="price-block"]//h2/text()'),
        financial_lease_term=val('//div[@data-testid="price-block"]//p[contains(text(),"mnd")]/text()'),
        advertentienummer=val('//div[contains(text(),"Advertentienummer")]/text()'),
        merk=spec("Merk"),
        model=spec("Model"),
        bouwjaar=spec("Bouwjaar"),
        km_stand=spec("Km stand"),
        transmissie=spec("Transmissie"),
        prijs=spec("Prijs"),
        brandstof=spec("Brandstof"),
        btw_marge=spec("Btw/marge"),
        opties_accessoires=", ".join(
            x.strip() for x in t('//h2[contains(.,"Opties")]/following-sibling::ul/li/text()')
        ) or None,
        address=val('//div[@class="flex justify-between"]/div/p/text()'),
        images = extract_images(tree)
        if not images:
            logging.warning(f"[no-images] {url}")
            return None            # skip cars that genuinely have zero photos


    )

# ───────────────────────── INSERT helpers ─────────────────────────
INS_LISTINGS = """
INSERT INTO car_listings (
 url,title,subtitle,financial_lease_price,financial_lease_term,
 advertentienummer,merk,model,bouwjaar,km_stand,
 transmissie,prijs,brandstof,btw_marge,opties_accessoires,address
) VALUES %s RETURNING id;
"""
INS_IMAGES = "INSERT INTO car_images (image_url,car_listing_id) VALUES %s"

def safe_execute_values(cur, sql, rows, **kw):
    """
    Tries execute_values bulk; on DataError inserts rows one-by-one
    and logs/skips the offenders.
    """
    try:
        psycopg2.extras.execute_values(cur, sql, rows, **kw)
        return []

    except psycopg2.Error as e:
        err_rows = []
        # per-row salvage
        for row in rows:
            try:
                psycopg2.extras.execute_values(cur, sql, [row], **kw)
            except psycopg2.Error:
                err_rows.append(row)
                cur.connection.rollback()     # clear error state
        return err_rows

def bulk_insert(cur, recs):
    skipped = []

    for i in range(0,len(recs),LISTING_CHUNK):
        chunk = recs[i:i+LISTING_CHUNK]

        listing_rows=[(
            r["url"], r["title"], r["subtitle"], r["financial_lease_price"],
            r["financial_lease_term"], r["advertentienummer"], r["merk"],
            r["model"], r["bouwjaar"], r["km_stand"], r["transmissie"],
            r["prijs"], r["brandstof"], r["btw_marge"],
            r["opties_accessoires"], r["address"]
        ) for r in chunk]

        bad = safe_execute_values(cur, INS_LISTINGS, listing_rows, page_size=500)
        ids  = [row[0] for row in cur.fetchall()] if not bad else []

        img_rows=[]
        for rec,lid in zip(chunk,ids):
            img_rows.extend((img,lid) for img in rec["images"])

        for j in range(0,len(img_rows),IMAGE_CHUNK):
            safe_execute_values(cur, INS_IMAGES, img_rows[j:j+IMAGE_CHUNK], page_size=1000)

        skipped.extend(bad)

    return skipped

# ───────────────────────── MAIN ─────────────────────────
def main():
    logging.info("scraper start")
    links = collect_links()

    conn, cur = psycopg2.connect(DB_DSN), None
    try:
        cur = conn.cursor()
        cur.execute("SELECT url FROM car_listings;")
        existing = {u for (u,) in cur.fetchall()}

        new_urls = list(links-existing)
        obsolete = list(existing-links)
        logging.info(f"new={len(new_urls)}  obsolete={len(obsolete)}")

        if obsolete:
            cur.execute("SELECT id FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            ids=[i for (i,) in cur.fetchall()]
            if ids:
                cur.execute("DELETE FROM car_images WHERE car_listing_id = ANY(%s)", (ids,))
            cur.execute("DELETE FROM car_listings WHERE url = ANY(%s)", (obsolete,))
            conn.commit()
            logging.info("obsolete deleted")

        # ─── parse + insert in batches ──────────────────────
        if new_urls:
            logging.info("scraping details batch-wise …")
            for i in range(0,len(new_urls),PARSE_CHUNK):
                batch = new_urls[i:i+PARSE_CHUNK]
                logging.info(f"[batch {i//PARSE_CHUNK+1}] urls={len(batch)}")

                with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
                    recs = [r for r in pool.map(scrape_detail,batch) if r]

                logging.info(f"[batch {i//PARSE_CHUNK+1}] parsed={len(recs)}  mem={mem_mb()} MB → inserting")
                skipped = bulk_insert(cur,recs); conn.commit()

                if skipped:
                    logging.warning(f"{len(skipped)} rows skipped due to DB errors")

                del recs; gc.collect()

        logging.info("scraper finished OK")

    finally:
        if cur: cur.close()
        conn.close()

if __name__ == "__main__":
    main()
