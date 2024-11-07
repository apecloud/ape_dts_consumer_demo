import requests
import time
import function as udf
from io import BytesIO
import fastavro
import avro_schema

# configs
URL = "http://127.0.0.1:10231"
BATCH_SIZE = 2

# init
schema = avro_schema.get_avro_schema()

# API: info
url = f"{URL}/info"
resp = requests.get(url)
info_data = resp.json()

print(f"acked_batch_id: {info_data["acked_batch_id"]}, sent_batch_id: {info_data["sent_batch_id"]}")
ack_batch_id = info_data["sent_batch_id"]

while True:
    # API: fetch_new
    url = f"{URL}/fetch_new?batch_size={BATCH_SIZE}&ack_batch_id={ack_batch_id}"
    resp = requests.get(url)
    fetch_data = resp.json()

    ack_batch_id = fetch_data["batch_id"]
    print(f"ack_batch_id: {ack_batch_id}")

    # parse and handle records
    for item in fetch_data["data"]:
        record = fastavro.schemaless_reader(BytesIO(bytes(item)), schema)
        udf.handle(record)

    if len(fetch_data["data"]) == 0:
        time.sleep(5)