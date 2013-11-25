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
	data := make([]byte, 4096*8*8)
	for {
		con, err := lis.Accept()
		if err != nil {
			continue
		}
		fmt.Println("(LS) Got a connection!")
		go func() {
			fmt.Println("(LS) Listening to outChannel!")
			for c, ok := <-outChannel; c.Type != Fatal && ok; c, ok = <-outChannel {
				// fmt.Println("(LS) Sending message through outChannel: " + c.ToString())
				con.Write([]byte(c.ToString()))
			}
		}()
		for n, err := con.Read(data); err == nil; n, err = con.Read(data) {
			msgs := ParseMessages(string(data[:n]))
			for _, m := range msgs {
				// fmt.Println("(LS) Got a message through the connection: " + m.ToString())
				inChannel <- m
			}
		}
		fmt.Println("(LS) Closing channels")
		outChannel <- FatalMessage()
		// close(outChannel)
		// close(inChannel)
	}
}

func DialAndListen(toRemote string, inChannel, outChannel chan Message) {
	// fmt.Println("Dialing and listening")
	var con net.Conn
	var err error
	for con, err = net.Dial("tcp", toRemote); err != nil; con, err = net.Dial("tcp", toRemote) {
		time.Sleep(100 * time.Millisecond)
	}

	go func() {
		for c, ok := <-outChannel; c.Type != Fatal && ok; c, ok = <-outChannel {
			// fmt.Println("(DAL) Sending message through outChannel: " + c.ToString())
			con.Write([]byte(c.ToString()))
		}
	}()

	fmt.Println("Got a connection!")
	data := make([]byte, 4096*8*8)
	for n, err := con.Read(data); err == nil; n, err = con.Read(data) {
		msgs := ParseMessages(string(data[:n]))
		for _, m := range msgs {
			// fmt.Println("(DAL) Got a message through the connection: " + m.ToString())
			inChannel <- m
		}
	}
	fmt.Println("(DAL) Closing channels")
	// close(inChannel)
}
