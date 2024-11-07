package main

import (
	"cubetran_udf_golang/metadata"
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro/v2"
)

func main() {
	// runKafkaDemo()
	runHttpDemo()
	// runHttpDemo2()
}

func bytes_to_struct(codec *goavro.Codec, bytes []byte) metadata.ApeDtsRecord {
	// kafka/http msg -> avro record
	native, _, _ := codec.NativeFromBinary(bytes)
	jsonData, _ := json.Marshal(native)
	fmt.Printf("avro record: %s\n", jsonData)

	// avro recod -> ape_dts record
	avroConverter := metadata.AvroConverter{}
	apeDtsRecord := avroConverter.AvroToApeDts(native.(map[string]interface{}))
	return apeDtsRecord
}
