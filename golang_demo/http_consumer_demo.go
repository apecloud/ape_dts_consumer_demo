package main

import (
	"bytes"
	"cubetran_udf_golang/metadata"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/linkedin/goavro/v2"
)

type InfoResp struct {
	AckedBatchID uint64 `json:"acked_batch_id"`
	SentBatchID  uint64 `json:"sent_batch_id"`
}

type FetchResp struct {
	Data    [][]byte `json:"data"`
	BatchID uint64   `json:"batch_id"`
}

type AckResp struct {
	AckedBatchID uint64 `json:"acked_batch_id"`
}

type AckReq struct {
	AckBatchID uint64 `json:"ack_batch_id"`
}

const URL string = "http://127.0.0.1:10231"
const BATCH_SIZE uint = 2

// extract data from HTTP server and decode
func runHttpDemo() {
	codec, _ := goavro.NewCodec(metadata.AvroSchema)

	// api: info
	url := fmt.Sprintf("%s/info", URL)
	infoResp := testInfo(url)

	ackBatchId := infoResp.SentBatchID
	for {
		// api: fetch_new
		url = fmt.Sprintf("%s/fetch_new?batch_size=%d&ack_batch_id=%d", URL, BATCH_SIZE, ackBatchId)
		fetchResp := testFetch(url, codec)
		ackBatchId = fetchResp.BatchID

		if len(fetchResp.Data) == 0 {
			time.Sleep(5 * time.Second)
		}
	}
}

// extract data from HTTP server and decode
func runHttpDemo2() {
	codec, _ := goavro.NewCodec(metadata.AvroSchema)

	for {
		// api: info
		url := fmt.Sprintf("%s/info", URL)
		testInfo(url)

		// api: fetch_new
		url = fmt.Sprintf("%s/fetch_new?batch_size=%d", URL, BATCH_SIZE)
		fetchResp := testFetch(url, codec)

		// api: fetch_old
		url = fmt.Sprintf("%s/fetch_old?old_batch_id=%d", URL, fetchResp.BatchID)
		testFetch(url, codec)

		// api: ack
		url = fmt.Sprintf("%s/ack", URL)
		ackReq := AckReq{
			AckBatchID: fetchResp.BatchID,
		}
		testAck(url, ackReq)

		if len(fetchResp.Data) == 0 {
			time.Sleep(5 * time.Second)
		}
	}
}

func testInfo(url string) InfoResp {
	infoResp := sendGet[InfoResp](url)

	jsonData, _ := json.Marshal(infoResp)
	fmt.Printf("info response: %s\n", jsonData)

	return infoResp
}

func testFetch(url string, codec *goavro.Codec) FetchResp {
	fetchResp := sendGet[FetchResp](url)

	fmt.Printf("fetched batch_id: %d\n", fetchResp.BatchID)
	for _, item := range fetchResp.Data {
		apeDtsRecord := bytes_to_struct(codec, item)
		jsonData, _ := json.Marshal(apeDtsRecord)
		fmt.Printf("ape_dts record: %s\n", jsonData)
	}

	return fetchResp
}

func testAck(url string, ackReq AckReq) AckResp {
	ackResp := sendPost[AckReq, AckResp](url, ackReq)

	jsonData, _ := json.Marshal(ackResp)
	fmt.Printf("ack response: %s\n", jsonData)

	return ackResp
}

func sendGet[T any](url string) T {
	fmt.Printf("HTTP GET url: %s\n", url)

	// send request
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Failed to send GET request: %v", err)
	}
	defer resp.Body.Close()

	return parseResponse[T](resp)
}

func sendPost[T1 any, T2 any](url string, payload T1) T2 {
	fmt.Printf("HTTP POST url: %s\n", url)

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error sending POST request: %v", err)
	}
	defer resp.Body.Close()

	return parseResponse[T2](resp)
}

func parseResponse[T any](resp *http.Response) T {
	// read response
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Received non-OK response: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response body: %v", err)
	}

	// parse response
	var result T
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	return result
}
