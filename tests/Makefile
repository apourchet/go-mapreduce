.PHONY: worker
worker: 
	go run workertest.go 127.0.0.2:3000

.PHONY: server
server: 
	go run servertest.go localhost:3000

.PHONY: all
all:
	go build -o executables/worker workertest.go
	go build -o executables/server servertest.go