package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/irqlevel/naiv/storage/proto"
	"google.golang.org/grpc"
)

const (
	address     = "localhost:50051"
	defaultName = "world"
)

type client struct {
	c pb.StorageClient
}

func (c *client) Ping(name string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.c.Ping(ctx, &pb.PingRequest{Name: name})
	if err != nil {
		return "", err
	}
	return r.Message, nil
}

func (c *client) InsertKey(name string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.c.InsertKey(ctx, &pb.InsertKeyRequest{Name: name, Value: value})
	if err != nil {
		return err
	}

	if r.Error != nil {
		return fmt.Errorf("%s", r.Error.Message)
	}

	return nil
}

func (c *client) GetKey(name string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.c.GetKey(ctx, &pb.GetKeyRequest{Name: name})
	if err != nil {
		return nil, err
	}

	if r.Error != nil {
		return nil, fmt.Errorf("%s", r.Error.Message)
	}

	return r.Value, nil
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Ldate | log.Lmicroseconds | log.Lshortfile)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := &client{c: pb.NewStorageClient(conn)}

	err = c.InsertKey("bla", []byte("Hello world!"))
	if err != nil {
		log.Printf("InsertKey error %v", err)
	}

	val, err := c.GetKey("bla")
	if err != nil {
		log.Printf("GetKey error %v", err)
	}

	log.Printf("key %s", string(val))
}
