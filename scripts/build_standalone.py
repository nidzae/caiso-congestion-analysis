"""
build_standalone.py
Produces a single self-contained HTML file with the visualization data embedded,
suitable for sharing via Google Drive, email, or any static hosting.
"""
import json
import os

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DASHBOARD_FILE = os.path.join(BASE_DIR, "viz", "dashboard.html")
DATA_FILE = os.path.join(BASE_DIR, "data", "visualization_data.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "viz", "caiso-congestion-dashboard.html")


def main():
    with open(DATA_FILE) as f:
        data = json.load(f)

    with open(DASHBOARD_FILE) as f:
        html = f.read()

    # Inject the data as a JS variable BEFORE the main script so it's
    # available when loadData() runs
    data_script = f"<script>const EMBEDDED_DATA = {json.dumps(data)};</script>\n"
    html = html.replace("<script>\n// Data will be loaded", data_script + "<script>\n// Data will be loaded")

    with open(OUTPUT_FILE, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"Built standalone dashboard: {OUTPUT_FILE}")
    print(f"File size: {size_kb:.0f} KB")
    print(f"Data: {len(data)} constraints embedded")
    print(f"\nThis file can be shared via:")
    print(f"  - Google Drive (upload, share link, recipients open in browser)")
    print(f"  - Email attachment")
    print(f"  - GitHub Pages")
    print(f"  - Any static web host (Netlify, Vercel, S3)")


if __name__ == "__main__":
    main()
