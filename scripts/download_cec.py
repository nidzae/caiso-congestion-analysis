"""
download_cec.py
Downloads the CEC California Electric Substations dataset (public, no auth required).
Source: California Energy Commission GIS Open Data Portal
"""
import requests
import json
import os

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "cec_substations.geojson")

CEC_URL = "https://cecgis-caenergy.opendata.arcgis.com/api/download/v1/items/93087e3b131d4301b34301e4e0168a2f/geojson?layers=0"

FEATURE_SERVICE_URL = (
    "https://services3.arcgis.com/bWPjFyq029ChCGur/arcgis/rest/services/"
    "California_Electric_Substations/FeatureServer/0/query"
)


def download_geojson():
    """Download full dataset as GeoJSON."""
    print("Downloading CEC substations dataset...")
    try:
        resp = requests.get(CEC_URL, timeout=120)
        if resp.status_code == 200:
            data = resp.json()
            with open(OUTPUT_FILE, "w") as f:
                json.dump(data, f)
            print(f"Downloaded {len(data.get('features', []))} substations")
            return data
        else:
            print(f"GeoJSON download failed ({resp.status_code}), trying feature service...")
            return download_via_feature_service()
    except Exception as e:
        print(f"GeoJSON download error: {e}, trying feature service...")
        return download_via_feature_service()


def download_via_feature_service():
    """Fallback: query the ArcGIS feature service with pagination."""
    all_features = []
    offset = 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": 2000,
        }
        resp = requests.get(FEATURE_SERVICE_URL, params=params, timeout=60)
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        offset += len(features)
        print(f"  Fetched {len(all_features)} substations so far...")

    geojson = {"type": "FeatureCollection", "features": all_features}
    with open(OUTPUT_FILE, "w") as f:
        json.dump(geojson, f)
    print(f"Downloaded {len(all_features)} substations total")
    return geojson


if __name__ == "__main__":
    download_geojson()
