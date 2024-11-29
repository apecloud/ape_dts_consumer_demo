use anyhow::bail;
use reqwest::blocking::{Client, Response};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::thread;
use std::time::Duration;

use crate::metadata::avro_converter::AvroConverter;

const URL: &str = "http://127.0.0.1:10231";
const BATCH_SIZE: u64 = 2;

#[derive(Serialize, Deserialize, Debug)]
struct InfoResp {
    acked_batch_id: u64,
    sent_batch_id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct FetchResp {
    data: Vec<Vec<u8>>, // Assuming data is a vector of byte vectors
    batch_id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct AckReq {
    ack_batch_id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct AckResp {
    acked_batch_id: u64,
}

pub fn run_http_demo() -> anyhow::Result<()> {
    let avro_converter = AvroConverter::new();
    let client = Client::new();

    let url = format!("{}/info", URL);
    let info_resp = test_info(&client, &url)?;

    let mut ack_batch_id = info_resp.sent_batch_id;
    loop {
        let url = format!(
            "{}/fetch_new?batch_size={}&ack_batch_id={}",
            URL, BATCH_SIZE, ack_batch_id
        );

        let fetch_resp = test_fetch(&client, &avro_converter, &url)?;
        ack_batch_id = fetch_resp.batch_id;

        if fetch_resp.data.is_empty() {
            println!("No data fetched, sleeping for 5 seconds...");
            thread::sleep(Duration::from_secs(5));
        }
    }
}

pub fn run_http_demo_2() -> anyhow::Result<()> {
    let avro_converter = AvroConverter::new();
    let client = Client::new();

    loop {
        // api: info
        let mut url = format!("{}/info", URL);
        test_info(&client, &url)?;

        // api: fetch_new
        url = format!("{}/fetch_new?batch_size={}", URL, BATCH_SIZE);
        let fetch_resp = test_fetch(&client, &avro_converter, &url)?;

        // api: fetch_old
        url = format!("{}/fetch_old?old_batch_id={}", URL, fetch_resp.batch_id);
        test_fetch(&client, &avro_converter, &url)?;

        // api: ack
        url = format!("{}/ack", URL);
        let ack_req: AckReq = AckReq {
            ack_batch_id: fetch_resp.batch_id,
        };
        test_ack(&client, &url, &ack_req)?;

        if fetch_resp.data.is_empty() {
            thread::sleep(Duration::from_secs(5));
        }
    }
}

fn test_info(client: &Client, url: &str) -> anyhow::Result<InfoResp> {
    let info_resp: InfoResp = send_get(client, url)?;
    println!("info response: {:?}", info_resp);
    Ok(info_resp)
}

fn test_fetch(
    client: &Client,
    avro_converter: &AvroConverter,
    url: &str,
) -> anyhow::Result<FetchResp> {
    let mut fetch_resp: FetchResp = send_get(client, url)?;

    println!("fetched batch_id: {}", fetch_resp.batch_id);
    for item in fetch_resp.data.iter_mut() {
        let ape_dts_record = avro_converter.avro_to_ape_dts(item)?;
        println!("ape_dts record: {}", json!(ape_dts_record))
    }

    Ok(fetch_resp)
}

fn test_ack(client: &Client, url: &str, ack_req: &AckReq) -> anyhow::Result<AckResp> {
    let ack_resp: AckResp = send_post(client, url, ack_req)?;
    println!("ack response: {}", json!(ack_resp));
    Ok(ack_resp)
}

fn send_get<T: for<'de> Deserialize<'de>>(client: &Client, url: &str) -> anyhow::Result<T> {
    println!("HTTP GET url: {}", url);
    let response: Response = client.get(url).send()?;
    parse_response(response)
}

fn send_post<T1: Serialize, T2: for<'de> Deserialize<'de>>(
    client: &Client,
    url: &str,
    payload: &T1,
) -> anyhow::Result<T2> {
    println!("HTTP POST url: {}", url);
    // Send the POST request
    let resp = client
        .request(Method::POST, url)
        .header("Content-Type", "application/json")
        .body(json!(payload).to_string())
        .send()?;
    // Parse the response
    parse_response::<T2>(resp)
}

fn parse_response<T: for<'de> Deserialize<'de>>(resp: Response) -> anyhow::Result<T> {
    if !resp.status().is_success() {
        bail!(format!(
            "bad http request, status: {}, resp: {:?}",
            resp.status(),
            resp.text()?
        ))
    }
    let result: T = serde_json::from_str(&resp.text()?)?;
    Ok(result)
}
