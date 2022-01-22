package main

import (
	"fmt"
	"log"
	"net"

	"github.com/kamijin-fanta/nbd-go"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	network, addr := "tcp", ":8888"
	fmt.Printf("listen on %s %s\n", network, addr)

	factory := &KSDeviceFactory{}

	lis, err := net.Listen(network, addr)
	if err != nil {
		panic(err)
	}

	err = nbd.ListenAndServe(lis, factory)
	if err != nil {
		panic(err)
	}
}
