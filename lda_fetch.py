import urllib.request, json, csv, time

url = "https://lda.senate.gov/api/v1/registrants/?limit=100"
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (research/public-data)"
}
rows = []

print("Fetching LDA registrants...")
while url:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        batch = data.get("results", [])
        rows.extend(batch)
        url = data.get("next")
        print(f"  {len(rows)} fetched...", end="\r")
        time.sleep(1.5)
    except Exception as e:
        print(f"\nError: {e} — retrying in 10s...")
        time.sleep(10)

print(f"\nTotal: {len(rows)}")

with open("lda_registrants.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["id","name","description","city","state"])
    w.writeheader()
    for r in rows:
        w.writerow({"id": r.get("id",""), "name": r.get("name",""),
                    "description": r.get("description",""),
                    "city": r.get("city",""), "state": r.get("state","")})

print("Saved lda_registrants.csv")
