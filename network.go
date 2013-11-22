package mapreduce

import (
	"bytes"
	"fmt"
	"net"
)

var (
	Verbosity = 0
)

func SetVerbosity(v int) {
	Verbosity = v
}

func Listen(inChannel chan []byte, remote string, closeChannel bool) {
	lis, err := net.Listen("tcp", remote)
	if err != nil {
		inChannel <- []byte{}
		return
	}
	defer lis.Close()
	if Verbosity > 0 {
		fmt.Println("Listening on remote: " + remote)
	}
	data := make([]byte, 5)
	for {
		output := bytes.NewBuffer([]byte{})
		con, err := lis.Accept()
		if err != nil {
			continue
		}
		for n, err := con.Read(data); err == nil; n, err = con.Read(data) {
			output.Write(data[0:n])
		}
		inChannel <- output.Bytes()
		if closeChannel {
			close(inChannel)
		}
		con.Close()
	}
}

func DialTest(fromRemote, toRemote, msg string) {
	message := Message{}
	message.Remote = fromRemote
	message.Type = Test
	message.Error = ""
	message.Message = msg
	con, err := net.Dial("tcp", toRemote)
	if err != nil {
		fmt.Printf("Host not found: %s\n", toRemote)
		// fmt.Printf("Host not found: %s\n", err)
		return
	}
	defer con.Close()
	if Verbosity > 0 {
		fmt.Println("Dialing remote: " + toRemote)
	}
	in, err := con.Write([]byte(message.ToString()))
	if err != nil {
		fmt.Printf("Error sending data: %s, in: %d\n", err, in)
	}
}

func DialMessage(message Message, toRemote string) {
	con, err := net.Dial("tcp", toRemote)
	if err != nil {
		fmt.Printf("Host not found: %s\n", toRemote)
		// fmt.Printf("Host not found: %s\n", err)
		return
	}
	defer con.Close()
	if Verbosity > 0 {
		fmt.Println("Dialing remote: " + toRemote)
	}
	in, err := con.Write([]byte(message.ToString()))
	if err != nil {
		fmt.Printf("Error sending data: %s, in: %d\n", err, in)
	}
}
