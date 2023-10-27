import fastavro
from io import BytesIO
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka import TopicPartition
import avro_schema
import function.function as udf
import config_util as cnf
import log as log

URL = "url"
TOPIC = "topic"
GROUP = "group"
OFFSET = "offset"

schema = avro_schema.get_avro_schema()

# load config
extractor, sinker, custom = cnf.parse_ini("./function/config.ini")

log.init(custom)
log.log("avro schema: " + str(schema))
log.log("config, extractor: " + str(extractor))
log.log("config, sinker: " + str(sinker))
log.log("config, custom: " + str(custom))

# consumer
consumer = KafkaConsumer(
    bootstrap_servers=[extractor.get(URL)],
    group_id=extractor.get(GROUP),
    auto_offset_reset="latest",
    enable_auto_commit=False
)
# partition, always 0
tp = TopicPartition(extractor.get(TOPIC), 0)
consumer.assign([tp])
# offset, user defined starting offset
if extractor.get(OFFSET) != None:
    consumer.seek(tp, int(extractor.get(OFFSET)))

# producer, if user need to sink modified records
producer = None
if sinker.get(URL) != None and sinker.get(TOPIC) != None:
    producer = KafkaProducer(
        bootstrap_servers=[sinker.get(URL)],
    )

for msg in consumer:
    # parse source kafka msg into record
    record = fastavro.schemaless_reader(BytesIO(msg.value), schema)

    # call user defined function to modify record
    user_records = udf.handle(record, custom)

    # write user records to target kafka if needed
    if producer != None and user_records != None:
        for user_record in user_records:
            writer = BytesIO()
            fastavro.schemaless_writer(writer, schema, user_record)
            producer.send(sinker.get(TOPIC), writer.getvalue(), key=msg.key)

    # commit offsets
    consumer.commit()

# Close consumer
consumer.close()