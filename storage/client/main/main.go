package main

import (
	"log"
	"os"

	"github.com/irqlevel/naiv/storage/client/client"
)

const (
	address     = "localhost:50051"
	defaultName = "world"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Ldate | log.Lmicroseconds | log.Lshortfile)

	c, err := client.NewClient(address)
	if err != nil {
		log.Fatalf("NewClient error %v\n", err)
	}
	defer c.Close()

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
