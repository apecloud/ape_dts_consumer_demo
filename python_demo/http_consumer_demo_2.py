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

while True:
    # API: info
    url = f"{URL}/info"
    resp = requests.get(url)
    info_data = resp.json()

    print(f"acked_batch_id: {info_data["acked_batch_id"]}, sent_batch_id: {info_data["sent_batch_id"]}")

    # API: fetch_new
    url = f"{URL}/fetch_new?batch_size={BATCH_SIZE}"
    resp = requests.get(url)
    fetch_data = resp.json()

    # sleep 5 seconds if no records fectched
    if len(fetch_data["data"]) == 0:
        time.sleep(5)

    # parse and handle records
    for item in fetch_data["data"]:
        record = fastavro.schemaless_reader(BytesIO(bytes(item)), schema)
        udf.handle(record)

    batch_id = fetch_data["batch_id"]
    print(f"batch_id: {batch_id}")

    # API: fetch_old
    url = f"{URL}/fetch_old?old_batch_id={batch_id}"
    resp = requests.get(url)
    fetch_data = resp.json()

    # parse and handle records
    for item in fetch_data["data"]:
        record = fastavro.schemaless_reader(BytesIO(bytes(item)), schema)
        udf.handle(record)

    # API: ack
    ack_req = {
        "ack_batch_id": batch_id,
    }
    url = f"{URL}/ack"
    resp = requests.post(url, json=ack_req)

    print(f"acked_batch_id: {resp.json()["acked_batch_id"]}")