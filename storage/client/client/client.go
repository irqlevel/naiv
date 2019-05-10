package client

import (
	"context"
	"fmt"
	"time"

	pb "github.com/irqlevel/naiv/storage/proto"
	"google.golang.org/grpc"
)

type Client struct {
	c    pb.StorageClient
	conn *grpc.ClientConn
}

func (c *Client) Ping(name string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.c.Ping(ctx, &pb.PingRequest{Name: name})
	if err != nil {
		return "", err
	}
	return r.Message, nil
}

func (c *Client) InsertKey(name string, value []byte) error {
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

func (c *Client) GetKey(name string) ([]byte, error) {
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

func (c *Client) Close() {
	c.conn.Close()
}

func NewClient(address string) (*Client, error) {
	c := &Client{}
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.c = pb.NewStorageClient(conn)
	c.conn = conn
	return c, nil
}
