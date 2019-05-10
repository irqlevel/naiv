BIN_OUT = naiv_storage_client

all: binary
	cp $(BIN_OUT) ../../bin/$(BIN_OUT)

binary:	main/main.go
	go build -o $(BIN_OUT) main/main.go

clean:
	rm -rf $(BIN_OUT)

