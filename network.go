package mapreduce

import (
	// "bytes"
	"fmt"
	"net"
	"time"
)

var (
	Verbosity = 0
)

func SetVerbosity(v int) {
	Verbosity = v
}

func ListenStream(inChannel, outChannel chan Message, remote string) {
	lis, err := net.Listen("tcp", remote)
	if err != nil {
		close(inChannel)
		return
	}
	defer lis.Close()
	if Verbosity > 0 {
		fmt.Println("Listening on remote: " + remote)
	}
	data := make([]byte, 4096*8)
	for {
		con, err := lis.Accept()
		if err != nil {
			continue
		}
		fmt.Println("Got a connection!")
		go func() {
			for c := <-outChannel; c.Type != Fatal; c = <-outChannel {
				fmt.Println("Sending message through outChannel!")
				con.Write([]byte(c.ToString()))
			}
		}()
		for n, err := con.Read(data); err == nil; n, err = con.Read(data) {
			msgs := ParseMessages(string(data[:n]))
			fmt.Println("Got a message through the connection!")
			for _, m := range msgs {
				inChannel <- m
			}
		}
		close(outChannel)
		close(inChannel)
	}
}

func DialAndListen(toRemote string, inChannel, outChannel chan Message) {
	fmt.Println("Dialing and listening")
	var con net.Conn
	var err error
	for con, err = net.Dial("tcp", toRemote); err != nil; con, err = net.Dial("tcp", toRemote) {
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("Got a connection!")
	go func() {
		for c := <-outChannel; c.Type != Fatal; c = <-outChannel {
			fmt.Println("Sending message through outChannel!")
			con.Write([]byte(c.ToString()))
		}
	}()

	data := make([]byte, 500)
	for n, err := con.Read(data); err == nil; n, err = con.Read(data) {
		msgs := ParseMessages(string(data[:n]))
		fmt.Println("Got a message through the connection!")
		for _, m := range msgs {
			inChannel <- m
		}
	}
	close(inChannel)
}
