package socketio

import (
	"bytes"
	"net"
)

func Listen(inChannel chan []byte, host, port string) {
	lis, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		inChannel <- []byte{}
		return
	}
	defer lis.Close()
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
		con.Close()
	}
}
