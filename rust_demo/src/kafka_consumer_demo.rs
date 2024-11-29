use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message,
};
use serde_json::json;

use crate::metadata::avro_converter::AvroConverter;

pub fn run_kafka_demo() -> anyhow::Result<()> {
    // Kafka consumer configuration
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9093")
        .set("group.id", "example_group")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe to the topic
    consumer.subscribe(&["test"])?;
    let avro_converter = AvroConverter::new();
    loop {
        // Poll for messages
        match consumer.poll(None) {
            Some(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    let ape_dts_record = avro_converter.avro_to_ape_dts(payload)?;
                    println!("ape_dts record: {}", json!(ape_dts_record))
                }
            }
            Some(Err(e)) => {
                eprintln!("Error while receiving message: {:?}", e);
            }
            None => {
                // No message received, continue polling
            }
        }
    }
}
