import requests
import pandas as pd
from urllib.parse import urlparse, parse_qs

BASE_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"

all_rows = []

params = {
    "size": 1000,
    "qs": "date_etablissement_dpe:[2021-01-01 TO 2024-12-31]",
    "select": "numero_dpe,date_etablissement_dpe,methode_application_dpe,etiquette_dpe,type_batiment,periode_construction,zone_climatique,code_insee_ban,typologie_logement,nombre_niveau_immeuble,nombre_niveau_logement,nombre_appartement,type_energie_principale_chauffage,type_energie_principale_ecs,type_ventilation,conso_5_usages_par_m2_ep,emission_ges_5_usages_par_m2,cout_chauffage,cout_ecs,cout_total_5_usages",
    "sort": "_id"
}

while True:
    resp = requests.get(BASE_URL, params=params)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text[:300]}")
        break

    data = resp.json()
    results = data.get("results", [])
    if not results:
        break

    all_rows.extend(results)
    print(f"Fetched {len(all_rows)} / {data['total']}")

    # Extract `after` cursor from the next URL
    next_url = data.get("next")
    if not next_url:
        break

    after_value = parse_qs(urlparse(next_url).query).get("after", [None])[0]
    if not after_value:
        break

    params["after"] = after_value

    if len(all_rows) % 100_000 == 0:

        pd.DataFrame(all_rows).to_csv("dpe_2021_2024_partial.csv", index=False)
        print(f"  → checkpoint saved")

df = pd.DataFrame(all_rows)
df.to_csv("dpe_2021_2024.csv", index=False)
print(f"Saved {len(df)} rows to dpe_2021_2024.csv")

print(df.shape)
print(df.head())