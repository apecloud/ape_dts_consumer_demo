import fastavro
from io import BytesIO
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka import TopicPartition
import avro_schema
import function as udf

URL = "localhost:9093"
TOPIC = "test"
GROUP = "ape_test"

# init
schema = avro_schema.get_avro_schema()

# consumer
consumer = KafkaConsumer(
    bootstrap_servers=[URL],
    group_id=GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=False
)
# partition, always 0
tp = TopicPartition(TOPIC, 0)
consumer.assign([tp])

for msg in consumer:
    # parse source kafka msg into record
    record = fastavro.schemaless_reader(BytesIO(msg.value), schema)

    # call user defined function to modify record
    user_records = udf.handle(record)

    # commit offsets
    consumer.commit()

# Close consumer
consumer.close()