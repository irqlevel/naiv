package kvmap

import (
	"fmt"
	"sync"
)

type MemoryKeyValueMap struct {
	KeyValueMap

	kvMap  map[string][]byte
	kvLock sync.RWMutex
}

func NewMemoryKeyValueMap() *MemoryKeyValueMap {
	return &MemoryKeyValueMap{kvMap: make(map[string][]byte)}
}

func (m *MemoryKeyValueMap) InsertKey(name string, value []byte) error {
	m.kvLock.Lock()
	defer m.kvLock.Unlock()

	_, ok := m.kvMap[name]
	if ok {
		return fmt.Errorf("Already exists")
	}

	tmp := make([]byte, len(value))
	copy(tmp, value)

	m.kvMap[name] = tmp
	return nil
}

func (m *MemoryKeyValueMap) GetKey(name string) ([]byte, error) {
	m.kvLock.RLock()
	defer m.kvLock.RUnlock()

	value, ok := m.kvMap[name]
	if !ok {
		return nil, fmt.Errorf("Not found")
	}

	tmp := make([]byte, len(value))
	copy(tmp, value)

	return tmp, nil
}
