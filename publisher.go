package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/stan.go"
)

var MODEL = `{"order_uid":"abc3feb7b2b84b6tesa","track_number":"WBILMTESTTRACK","entry":"WBIL","delivery":{"name":"Test Testov","phone":"+9720000000","zip":"2639809","city":"Kiryat Mozkin","address":"Ploshad Mira 15","region":"Kraiot","email":"test@gmail.com"},"payment":{"transaction":"b563feb7b2b84b6test","request_id":"","currency":"USD","provider":"wbpay","amount":1817,"payment_dt":1637907727,"bank":"alpha","delivery_cost":1500,"goods_total":317,"custom_fee":0},"items":[{"chrt_id":9934930,"track_number":"WBILMTESTTRACK","price":453,"rid":"ab4219087a764ae0btest","name":"Mascaras","sale":30,"size":"0","total_price":317,"nm_id":2389212,"brand":"Vivienne Sabo","status":202}],"locale":"en","internal_signature":"","customer_id":"test","delivery_service":"meest","shardkey":"9","sm_id":99,"date_created":"2021-11-26T06:22:19Z","oof_shard":"1"}`

type Order struct {
	OrderUid  string `json:"order_uid"`
	FullOrder string `json:"full_order"`
}

func main() {

	urlOption := stan.NatsURL(fmt.Sprintf("nats://%s:%s", os.Getenv("NATS_HOST"), os.Getenv("NATS_PORT")))

	sc, err := stan.Connect("test-cluster", "stan-pub", urlOption)
	if err != nil {
		log.Fatalf("nats streaming connection failed: %v\n", err)
	}

	var order Order

	err = json.Unmarshal([]byte(MODEL), &order)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}
	fmt.Println("orderUid:", order.OrderUid)
	order.FullOrder = string(MODEL)
	fmt.Println("FullOrder:", order.FullOrder)

	err = sc.Publish("channel", []byte(MODEL))
	fmt.Printf("Published!\n")
	if err != nil {
		fmt.Printf("Publishing error! %v", err)
		os.Exit(1)
	}

}
