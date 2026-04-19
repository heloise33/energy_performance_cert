import requests
import csv
import os
import time
from datetime import date
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs

BASE_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
OUTPUT_DIR = "dpe_chunks"
FINAL_FILE = "dpe_2021_2026.csv"
PAGE_SIZE = 1000
MAX_WORKERS = 4

FIELDS = ",".join([
    "numero_dpe", "date_etablissement_dpe", "methode_application_dpe",
    "etiquette_dpe", "type_batiment", "periode_construction", "zone_climatique",
    "code_insee_ban", "typologie_logement", "nombre_niveau_immeuble",
    "nombre_niveau_logement", "nombre_appartement",
    "type_energie_principale_chauffage", "type_energie_principale_ecs",
    "type_ventilation", "conso_5_usages_par_m2_ep",
    "emission_ges_5_usages_par_m2", "cout_chauffage", "cout_ecs",
    "cout_total_5_usages",
    "surface_habitable_logement", "surface_habitable_immeuble",
])

DATA_START = date(2021, 1, 1)
DATA_END   = date(2026, 1, 1)

os.makedirs(OUTPUT_DIR, exist_ok=True)


def date_chunks(start: date, end: date, months=6):
    chunks, cur = [], start
    while cur < end:
        nxt = min(cur + relativedelta(months=months), end)
        chunks.append((cur.isoformat(), nxt.isoformat()))
        cur = nxt
    return chunks


def fetch_with_retry(params, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=60)
            if resp.status_code == 200:
                return resp
            print(f"  http {resp.status_code}, retry {attempt + 1}/{max_retries}")
        except Exception as e:
            print(f"  {e}, retry {attempt + 1}/{max_retries}")
        time.sleep(2 ** attempt)
    return None


def download_chunk(start_date, end_date):
    out = f"{OUTPUT_DIR}/dpe_{start_date}_{end_date}.csv"
    if os.path.exists(out) and os.path.getsize(out) > 0:
        print(f"skip {start_date} → {end_date}")
        return out

    params = {
        "size": PAGE_SIZE,
        "select": FIELDS,
        "sort": "_id",
        "qs": f"date_etablissement_dpe:[{start_date} TO {end_date}]",
    }

    total = 0
    chunk_total = "?"
    writer = None

    try:
        with open(out, "w", newline="", encoding="utf-8") as f:
            while True:
                resp = fetch_with_retry(params)
                if resp is None:
                    raise Exception("max retries exceeded")

                data = resp.json()
                results = data.get("results", [])
                if not results:
                    break

                if writer is None:
                    chunk_total = data.get("total", "?")
                    writer = csv.DictWriter(
                        f,
                        fieldnames=FIELDS.split(","),
                        extrasaction="ignore",
                        restval="",
                    )
                    writer.writeheader()

                writer.writerows(results)
                total += len(results)
                print(f"  {start_date}: {total:>10,} / {chunk_total:,}")

                next_url = data.get("next")
                if not next_url:
                    break
                after = parse_qs(urlparse(next_url).query).get("after", [None])[0]
                if not after:
                    break
                params["after"] = after

    except Exception as e:
        if os.path.exists(out):
            os.remove(out)
        raise Exception(f"{start_date}→{end_date} failed: {e}")

    print(f"{start_date} → {end_date}: {total:,} rows")
    return out


def merge(output_dir, final_file):
    files = sorted(
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.endswith(".csv")
    )
    total = 0
    writer = None

    with open(final_file, "w", newline="", encoding="utf-8") as fout:
        for path in files:
            with open(path, newline="", encoding="utf-8") as fin:
                reader = csv.DictReader(fin)
                if writer is None:
                    writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
                    writer.writeheader()
                rows = list(reader)
                writer.writerows(rows)
                total += len(rows)
            print(f"{os.path.basename(path)} → {total:,} rows total")

    print(f"\ndone: {total:,} rows in {final_file}")


if __name__ == "__main__":
    chunks = date_chunks(DATA_START, DATA_END, months=6)
    print(f"{len(chunks)} chunks, {MAX_WORKERS} workers\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_chunk, s, e): (s, e) for s, e in chunks}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as ex:
                print(f"failed {ex}")

    done = {f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")}
    expected = {f"dpe_{s}_{e}.csv" for s, e in chunks}
    missing = expected - done
    if missing:
        print(f"\nmissing {len(missing)} chunks, re-run:")
        for f in sorted(missing):
            print(f"  {f}")
    else:
        print("\nmerging...")
        merge(OUTPUT_DIR, FINAL_FILE)