import requests
import csv
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

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

    print(f"Fetching page {page + 1}...")
    response = requests.get(API_URL, params=params, headers={"Accept": "application/json"})

    if response.status_code != 200:
        print(f"Failed to fetch page {page + 1}: {response.status_code}")
        break

    data = response.json()
    if not data:
        print("No data returned.")
        break

    unids = [entry.get("@unid") for entry in data if entry.get("@unid")]
    print(f"Found {len(unids)} unids on page {page + 1}.")

    with ThreadPoolExecutor(max_workers=20) as executor:
        results_buffer = list(executor.map(fetch_detail, unids))
        for result in results_buffer:
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
