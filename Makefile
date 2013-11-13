.PHONY: controller
controller:
	cd executables && go build ../testserver.go
	executables/testserver 127.0.0.1:9998

.PHONY: worker
worker: 
	cd executables && go build ../testsocket.go
	executables/testsocket 127.0.0.2:9998 127.0.0.1:9998
