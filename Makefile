.PHONY: controller
controller:
	go run testserver.go 127.0.0.1:9998

.PHONY: worker
worker: 
	go run testsocket.go 127.0.0.2:9998 127.0.0.1:9998
