package main

import (
	//"code.google.com/p/go.net/websocket"
	"fmt"
	"net"
	"os"
)

type T struct {
	Msg   string
	Count int
}

func main() {
	var (
		host          = "127.0.0.1"
		port          = "9998"
		remote        = host + ":" + port
		msg    string = "Some message here"
	)
	con, error := net.Dial("tcp", remote)
	defer con.Close()
	if error != nil {
		fmt.Printf("Host not found: %s\n", error)
		os.Exit(1)
	}

	in, error := con.Write([]byte(msg))
	if error != nil {
		fmt.Printf("Error sending data: %s, in: %d\n", error, in)
		os.Exit(2)
	}

	fmt.Println("Connection OK")
	// receive JSON type T
	//var data T
	//websocket.JSON.Receive(ws, &data)
}
