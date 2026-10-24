package main

import (
	"flag"
	"fmt"
)

func main() {
	broker := flag.String("broker", "", "broker")
	topic := flag.String("topic", "", "topic")
	flag.Parse()
	fmt.Printf("ackhammer-consumer broker=%s topic=%s\n", *broker, *topic)
}
