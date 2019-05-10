package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	kvmap "github.com/irqlevel/naiv/lib/common/kvmap"
	"github.com/irqlevel/naiv/storage/client/client"
	pb "github.com/irqlevel/naiv/storage/proto"
	"google.golang.org/grpc"
)

type LogConfig struct {
	FileName string `toml:"file_name"`
}

type IdentityConfig struct {
	Uuid string
	Host string
}

type ClusterConfig struct {
	Hosts []string
}

type ServerConfig struct {
	Identity IdentityConfig
	Log      LogConfig
	Cluster  ClusterConfig
}

type Server struct {
	kvMap           kvmap.KeyValueMap
	config          *ServerConfig
	grpcServer      *grpc.Server
	signalChan      chan os.Signal
	errorChan       chan error
	heartbeatTicker *time.Ticker
}

func (s *Server) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingReply, error) {
	return &pb.PingReply{Message: "hello " + in.Name + " i'am " + s.config.Identity.Uuid}, nil
}

func (s *Server) InsertKey(ctx context.Context, in *pb.InsertKeyRequest) (*pb.InsertKeyReply, error) {
	err := s.kvMap.InsertKey(in.Name, in.Value)
	if err != nil {
		return &pb.InsertKeyReply{Error: &pb.Error{Message: err.Error()}}, nil
	} else {
		return &pb.InsertKeyReply{}, nil
	}
}

func (s *Server) GetKey(ctx context.Context, in *pb.GetKeyRequest) (*pb.GetKeyReply, error) {
	value, err := s.kvMap.GetKey(in.Name)
	if err != nil {
		return &pb.GetKeyReply{Error: &pb.Error{Message: err.Error()}}, nil
	} else {
		return &pb.GetKeyReply{Value: value}, nil
	}
}

func (s *Server) Shutdown() {
	log.Printf("shutdowning\n")
	s.heartbeatTicker.Stop()
	s.grpcServer.GracefulStop()
	log.Printf("shutdown complete\n")
}

func (s *Server) Heartbeat() {
	log.Printf("heartbeat\n")
	for _, addr := range s.config.Cluster.Hosts {
		if addr != s.config.Identity.Host {
			client, err := client.NewClient(addr)
			if err != nil {
				log.Printf("can't connect %s error %v\n", addr, err)
				continue
			}

			msg, err := client.Ping(s.config.Identity.Uuid)
			if err != nil {
				log.Printf("addr %s ping error %v\n", addr, err)
				client.Close()
				continue
			}
			log.Printf("addr %s ping message %s\n", addr, msg)
			client.Close()
		}
	}
	log.Printf("heartbeat done\n")
}

func NewServer(config *ServerConfig) (*Server, error) {
	return &Server{config: config, kvMap: kvmap.NewMemoryKeyValueMap()}, nil
}

func (s *Server) Run() {
	logFile, err := os.OpenFile(s.config.Log.FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("failed to open log file %s: %v\n", s.config.Log.FileName, err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	log.Printf("uuid %s\n", s.config.Identity.Uuid)
	log.Printf("host %s\n", s.config.Identity.Host)

	lis, err := net.Listen("tcp", s.config.Identity.Host)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterStorageServer(s.grpcServer, s)

	s.signalChan = make(chan os.Signal, 1)
	s.errorChan = make(chan error, 1)
	signal.Notify(s.signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("serving requests\n")
		err = s.grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("failed to serve: %v\n", err)
		}
	}()

	s.heartbeatTicker = time.NewTicker(10 * time.Second)
	for {
		select {
		case <-s.errorChan:
			log.Printf("received error %v\n", err)
			s.Shutdown()
			return
		case <-s.signalChan:
			log.Printf("received signal\n")
			s.Shutdown()
			return
		case <-s.heartbeatTicker.C:
			s.Heartbeat()
		}
	}
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Ldate | log.Lmicroseconds | log.Lshortfile)

	var config ServerConfig
	_, err := toml.DecodeFile("./config.toml", &config)
	if err != nil {
		log.Fatalf("failed to load config file %v\n", err)
	}

	s, err := NewServer(&config)
	if err != nil {
		log.Fatalf("failed to create server %v\n", err)
	}

	s.Run()
}
