BIN_OUT = naiv_storage_server

all: main.go
	go build -o $(BIN_OUT) main.go
	cp $(BIN_OUT) ../../bin/$(BIN_OUT)

clean:
	rm -rf $(BIN_OUT)

