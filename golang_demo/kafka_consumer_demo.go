package main

import (
	"cubetran_udf_golang/metadata"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
)

// extract data from kafka and decode
func runKafkaDemo() {
	// kafka consumer
	consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
		"group.id":          "example_group",
		"auto.offset.reset": "earliest",
	})
	_ = consumer.Subscribe("test", nil)

	// Avro codec
	codec, _ := goavro.NewCodec(metadata.AvroSchema)
	for {
		msg, _ := consumer.ReadMessage(-1)
		apeDtsRecord := bytes_to_struct(codec, msg.Value)
		jsonData, _ := json.Marshal(apeDtsRecord)
		fmt.Printf("ape_dts record: %s\n", jsonData)
	}
}
