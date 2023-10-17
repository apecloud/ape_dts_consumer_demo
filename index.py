import fastavro
from io import BytesIO
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
import avro_schema
import function.function as udf
import config_util as cnf

schema = avro_schema.get_avro_schema()
print("avro schema: ", schema)

source, target = cnf.parse_ini("./config.ini")

consumer = KafkaConsumer(
     source[cnf.TOPIC], 
     bootstrap_servers=[source[cnf.URL]],
     group_id=source[cnf.GROUP],
     auto_offset_reset=source[cnf.OFFSET],
     enable_auto_commit=False
)

producer = KafkaProducer(
    bootstrap_servers=[target[cnf.URL]],
)

for msg in consumer:
    # parse source kafka msg into record
    record = fastavro.schemaless_reader(BytesIO(msg.value), schema)

    # call user defined function to modify record
    user_records = udf.handle(record)

    # write user records back to target kafka if needed
    if user_records != None:
        for user_record in user_records:
            writer = BytesIO()
            fastavro.schemaless_writer(writer, schema, user_record)
            producer.send(target[cnf.TOPIC], writer.getvalue(), key=msg.key)

    # commit offsets
    consumer.commit()

# Close consumer
consumer.close()