import json

from spark_processing import process_file
from utils.S3Adapter import S3Adapter
import config

s3Adapter = S3Adapter(config.AWS_CREDENTIALS, config.S3_OUTPUT_BUCKET)
zones_list = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

categories_map = dict()
for zone in zones_list:
    data = s3Adapter.get_file(f"copycat_inc/data/{zone}_category_id.json")
    for category in data["items"]:
        categories_map[category["id"]] = category["snippet"]["title"]

for zone in zones_list:
    print(f"\nStarted processing {zone} zone")
    process_file(zone, f'{S3_DATA_LOCATION}{zone}videos.csv', categories_map, s3Adapter)
