package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	var (
		host   = "127.0.0.1"
		port   = "9998"
		remote = host + ":" + port
		data   = make([]byte, 5)
	)
	fmt.Println("Initiating server... (Ctrl-C to stop)")

	lis, error := net.Listen("tcp", remote)
	defer lis.Close()
	if error != nil {
		fmt.Printf("Error creating listener: %s\n", error)
		os.Exit(1)
	}
	for {
		var response string
		var read = true
		// fmt.Println("Here")
		con, error := lis.Accept()
		// fmt.Println("Here1")
		if error != nil {
			fmt.Printf("Error: Accepting data: %s\n", error)
			os.Exit(2)
		}
		fmt.Printf("=== New Connection received from: %s \n", con.RemoteAddr())
		for read {
			n, error := con.Read(data)
			fmt.Println("Read 5 more bytes")
			switch error {
			case nil:
				response = response + string(data[0:n])
			default:
				if error.Error() == "EOF" {
					fmt.Println("EOF")
					read = false
				} else {
					fmt.Printf("Error: Reading data : %s \n", error)
					read = false
				}
			}
		}
		fmt.Println("Data send by client: " + response)
		con.Close()
	}
}
