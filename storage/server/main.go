package main

import (
	"context"
	"log"
	"net"
	"os"

	kvmap "github.com/irqlevel/naiv/lib/common/kvmap"
	pb "github.com/irqlevel/naiv/storage/proto"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

type server struct {
	kvMap kvmap.KeyValueMap
}

func (s *server) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingReply, error) {
	return &pb.PingReply{Message: "Hello " + in.Name}, nil
}

func (s *server) InsertKey(ctx context.Context, in *pb.InsertKeyRequest) (*pb.InsertKeyReply, error) {
	err := s.kvMap.InsertKey(in.Name, in.Value)
	if err != nil {
		return &pb.InsertKeyReply{Error: &pb.Error{Message: err.Error()}}, nil
	} else {
		return &pb.InsertKeyReply{}, nil
	}
}

func (s *server) GetKey(ctx context.Context, in *pb.GetKeyRequest) (*pb.GetKeyReply, error) {
	value, err := s.kvMap.GetKey(in.Name)
	if err != nil {
		return &pb.GetKeyReply{Error: &pb.Error{Message: err.Error()}}, nil
	} else {
		return &pb.GetKeyReply{Value: value}, nil
	}
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Ldate | log.Lmicroseconds | log.Lshortfile)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterStorageServer(s, &server{kvMap: kvmap.NewMemoryKeyValueMap()})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
