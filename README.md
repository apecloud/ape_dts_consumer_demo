# Overview

ape_dts supports two consumer modes:

- ape_dts extracts snapshot/increment data from MySQL/Postgres, [sends it to Kafka in avro format](https://github.com/apecloud/cubetran-core/blob/feat/modify_docs/docs/en/tutorial/mysql_to_kafka_consumer.md). Users can consume it through Kafka clients.

- ape_dts [starts as an HTTP server](https://github.com/apecloud/cubetran-core/blob/feat/modify_docs/docs/en/tutorial/mysql_to_http_server_consumer.md), extracts snapshot/increment data from MySQL/Postgres and caches it in memory. Users can pull and consume the data in avro format through HTTP APIs.

# Quick Start
[golang consumer demo readme](/golang_demo/README.md)

[python consumer demo readme](/python_demo/README.md)