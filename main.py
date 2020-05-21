from spark_processing import process_file
import json

zones_list = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

categories_map = dict()
for zone in zones_list:
    with open(f'data/{zone}_category_id.json') as json_file:
        data = json.load(json_file)
        for category in data["items"]:
            categories_map[category["id"]] = category["snippet"]["title"]

for zone in zones_list:
    print(f"\nStarted processing {zone} zone")
    process_file(zone, f'data/{zone}videos.csv', categories_map)
