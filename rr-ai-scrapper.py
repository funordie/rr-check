import requests
import csv
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://ntr.tourism.government.bg/CategoryzationAll.nsf/api/data/collections/name/vRegistarMNValid1"
DETAIL_URL_PREFIX = "https://ntr.tourism.government.bg/CategoryzationAll.nsf/detail.xsp?id="
PAGE_SIZE = 100
results = []
all_keys = set()


def fetch_detail(unid):
    detail_url = DETAIL_URL_PREFIX + unid
    try:
        detail_res = requests.get(detail_url)
        if detail_res.status_code != 200:
            print(f"Failed to fetch detail for {unid}")
            return None

        soup = BeautifulSoup(detail_res.text, 'html.parser')
        sobstvenik_elem = soup.find(id="view:_id1:computedField30")
        sobstvenik = sobstvenik_elem.get_text(strip=True) if sobstvenik_elem else ""

        stopanisvast_elem = soup.find(id="view:_id1:computedField31")
        stopanisvast = stopanisvast_elem.get_text(strip=True) if stopanisvast_elem else ""

        data_block_elem = soup.find(class_="tab-content p30")
        key_values = {}
        if data_block_elem:
            rows = data_block_elem.find_all(class_=["rowtbl1", "rowtbl2"])
            for row in rows:
                left = row.find(class_="leftcol")
                right = row.find(class_="rightcol")
                if left:
                    key = left.get_text(strip=True)
                    value = right.get_text(strip=True) if right else ""
                    if key:
                        key_values[key] = value
                        all_keys.add(key)

        # Format data block, omit ':' if value is empty
        data_block = "\n".join(f"{k}: {v}" if v else k for k, v in key_values.items()) if key_values else ""

        result = {
            'id': unid,
            'url': detail_url,
            'sobstvenik': sobstvenik,
            'stopanisvast': stopanisvast,
            'data': data_block
        }
        result.update(key_values)
        return result

    except Exception as e:
        print(f"Error processing {unid}: {e}")
        return None


page = 0
while True:
    params = {
        'ps': PAGE_SIZE,
        'page': page,
        'sortcolumn': 'CNumber',
        'sortorder': 'ascending'
    }

    print(f"Fetching page {page + 1} (page param={page})...")
    response = requests.get(API_URL, params=params, headers={"Accept": "application/json"})

    if response.status_code != 200:
        print(f"Failed to fetch page {page + 1}: {response.status_code}")
        break

    data = response.json()
    if not data:
        print("No more data. Stopping.")
        break

    unids = [entry.get("@unid") for entry in data if entry.get("@unid")]
    if not unids:
        print("No unids found on page. Stopping.")
        break

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_unid = {executor.submit(fetch_detail, unid): unid for unid in unids}
        for future in as_completed(future_to_unid):
            result = future.result()
            if result:
                results.append(result)

    page += 1

# Prepare CSV headers
base_fields = ['id', 'url', 'sobstvenik', 'stopanisvast', 'data']
all_keys_sorted = sorted(all_keys)
fieldnames = base_fields + all_keys_sorted

# Save to CSV
output_file = "tourism_links_all_pages.csv"
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"Saved {len(results)} entries to {output_file}")

# Save human-readable HTML preview (first 10 entries)
html_file = "tourism_data_preview.html"
with open(html_file, 'w', encoding='utf-8') as f:
    f.write("<html><head><meta charset='utf-8'><title>Tourism Data Preview</title>")
    f.write("<style>body { font-family: Arial; } pre { background: #f9f9f9; padding: 10px; } h3 { color: #004488; }</style>")
    f.write("</head><body><h1>Tourism Data Preview</h1>")
    for i, entry in enumerate(results[:10], 1):
        f.write(f"<h3>Entry #{i}</h3>")
        f.write(f"<p><strong>ID:</strong> {entry['id']}<br>")
        f.write(f"<strong>URL:</strong> <a href='{entry['url']}' target='_blank'>{entry['url']}</a><br>")
        f.write(f"<strong>Sobstvenik:</strong> {entry['sobstvenik']}<br>")
        f.write(f"<strong>Stopanisvast:</strong> {entry['stopanisvast']}</p>")
        f.write("<pre>")
        f.write(entry['data'])
        f.write("</pre><hr>")
    f.write("</body></html>")

print(f"Saved HTML preview to {html_file}")
