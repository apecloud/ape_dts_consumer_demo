use http_consumer_demo::{run_http_demo, run_http_demo_2};
use kafka_consumer_demo::run_kafka_demo;

mod http_consumer_demo;
mod kafka_consumer_demo;
mod metadata;

fn main() {
    run_kafka_demo().unwrap();
    // run_http_demo().unwrap();
    // run_http_demo_2().unwrap();
}
