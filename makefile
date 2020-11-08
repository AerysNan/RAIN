all: clean protoc bin

protoc:
	cd proto/manager && protoc --go_out=. --go-grpc_out=. *.proto
	cd proto/worker && protoc --go_out=. --go-grpc_out=. *.proto

bin:
	cd cmd/client && go build -o ../../build/client
	cd cmd/manager && go build -o ../../build/manager
	cd cmd/worker && go build -o ../../build/worker

clean:
	rm -rf build/data/*