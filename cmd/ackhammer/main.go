package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("ackhammer - hammer your event consumers")
		fmt.Println("Usage: ackhammer run|replay|report|list|init")
		return
	}
	switch os.Args[1] {
	case "run":
		fmt.Println("running scenarios...")
	case "replay":
		fmt.Println("replaying report...")
	case "report":
		fmt.Println("generating report...")
	case "list":
		fmt.Println("faults|invariants")
	case "init":
		fmt.Println("scaffolding...")
	default:
		fmt.Printf("unknown command: %s\n", os.Args[1])
	}
}
