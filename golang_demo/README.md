# Overview
Kafka demo: receives and parses kafka records sent by ape_dts in avro format.

Http demo: fetches and parses records from ape_dts HTTP server in avro format.

# Quick Start
## Kafka demo
- 1, start [ape_dts task](https://github.com/apecloud/cubetran-core/blob/main/docs/en/tutorial/mysql_to_kafka_consumer.md) to send data to Kafka.
- 2, update configs in kafka_consumer_demo.go.
- 3, when received records sent by ape_dts, the outputs are like:

```
ape_dts record: {"Schema":"test_db","Tb":"","Operation":"ddl","Fields":null,"Before":null,"After":null,"Extra":{"db_type":"mysql","ddl_type":"create_table","query":"create table tb_1(id int, value int, primary key(id))"}}
ape_dts record: {"Schema":"test_db","Tb":"tb_1","Operation":"insert","Fields":[{"Name":"id","ColumnType":"int","AvroType":"Long"},{"Name":"value","ColumnType":"int","AvroType":"Long"}],"Before":null,"After":{"id":1,"value":1},"Extra":null}
ape_dts record: {"Schema":"test_db","Tb":"tb_1","Operation":"update","Fields":[{"Name":"id","ColumnType":"int","AvroType":"Long"},{"Name":"value","ColumnType":"int","AvroType":"Long"}],"Before":{"id":1,"value":1},"After":{"id":1,"value":2},"Extra":null}
ape_dts record: {"Schema":"test_db","Tb":"tb_1","Operation":"delete","Fields":[{"Name":"id","ColumnType":"int","AvroType":"Long"},{"Name":"value","ColumnType":"int","AvroType":"Long"}],"Before":{"id":1,"value":2},"After":null,"Extra":null}
```

## Http demo
- 1, start [ape_dts task](https://github.com/apecloud/cubetran-core/blob/main/docs/en/tutorial/mysql_to_http_server_consumer.md) as Http server.
- 2, update configs in http_consumer_demo.go.
- 3, when fetched records from ape_dts, the outputs are same as the Kafka demo.
