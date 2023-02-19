package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
)

type Order struct {
	OrderUid  string `json:"order_uid"`
	FullOrder string `json:"full_order"`
}

var cache map[string]string

func postgresWrite(message *stan.Msg, db *pgx.Conn) error {

	var order Order

	err := json.Unmarshal(message.Data, &order)
	order.FullOrder = string(message.Data)

	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Received message! order_uid: %s\n", order.OrderUid)

	_, err = db.Exec(context.Background(), "insert into json_order(order_uid, full_order) values($1,$2)",
		order.OrderUid,
		order.FullOrder)

	if err != nil {
		fmt.Println("error insert:", err)
	}

	cache[order.OrderUid] = string(message.Data)

	return nil
}

func runHTTP() {
	http.HandleFunc("/", handleRequest)
	http.HandleFunc("/listUids", handleList)
	http.ListenAndServe("localhost:8000", nil)
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
	id, ok := req.URL.Query()["uid"]

	if !ok {
		fmt.Fprintf(w, "400 Bad Request\n")
		return
	}

	res, ok := cache[id[0]]

	w.Header().Set("Access-Control-Allow-Origin", "*")

	if ok {
		fmt.Fprintf(w, "%s\n", res)
	} else {
		fmt.Fprintf(w, "404 No order with uid %s!\n", id[0])
	}
}

func handleList(w http.ResponseWriter, req *http.Request) {
	var res []string
	for i := range cache {
		res = append(res, i)
	}
	fmt.Fprintf(w, "%v\n", res)
}

func main() {

	cache = make(map[string]string)

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	items, err := conn.Query(ctx, "SELECT order_uid, full_order FROM json_order;")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	noItems := 0
	for items.Next() {
		var tmpOrder Order
		noItems += 1
		err := items.Scan(&tmpOrder.OrderUid, &tmpOrder.FullOrder)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		cache[tmpOrder.OrderUid] = tmpOrder.FullOrder
	}
	fmt.Printf("%d item(s) received!\n", noItems)

	if err != nil {
		fmt.Println(err)
	}

	urlOption := stan.NatsURL(fmt.Sprintf("nats://%s:%s", os.Getenv("NATS_HOST"), os.Getenv("NATS_PORT")))
	sc, err := stan.Connect("test-cluster", "stan-sub", urlOption)

	if err != nil {
		fmt.Printf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, stan.DefaultNatsURL)
	}

	startOpt := stan.StartAt(pb.StartPosition_NewOnly)
	channel := "channel"
	psqlw := func(msg *stan.Msg) {
		postgresWrite(msg, conn)
	}

	sub, err := sc.QueueSubscribe(channel, "", psqlw, startOpt, stan.DurableName(""))
	if err != nil {
		sc.Close()
	}

	go runHTTP()

	// Exit is triggered by an interrupt

	signalChan := make(chan os.Signal, 1)
	exitChan := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for range signalChan {
			fmt.Printf("\nReceived an interrupt! Closing connection...\n")
			sub.Unsubscribe()
			sc.Close()
			exitChan <- true
		}
	}()
	<-exitChan

}
