package kvmap

type KeyValueMap interface {
	InsertKey(name string, value []byte) error
	GetKey(name string) ([]byte, error)
}
