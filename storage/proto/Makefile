all: service.proto
	protoc --go_out=plugins=grpc:. ./service.proto --proto_path .

clean:
	rm -rf service.pb.go