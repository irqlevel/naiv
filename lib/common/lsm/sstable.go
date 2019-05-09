package lsm

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	log "github.com/irqlevel/naiv/lib/common/log"
)

var (
	ErrDeleted = fmt.Errorf("Deleted")
)

const (
	keysPerIndex = 512
)

type SsTable struct {
	filePath string
	file     *os.File
	lock     sync.RWMutex

	keyToOffset map[string]int64
	keys        []string

	minKey *string
	maxKey *string
	log    log.LogInterface
}

func (st *SsTable) index() error {
	file, err := os.OpenFile(st.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	st.minKey = nil
	st.maxKey = nil

	i := int64(0)

	st.keys = make([]string, 0)
	st.keyToOffset = make(map[string]int64)

	for {
		node := new(LsmNode)
		offset, err := file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		err = node.ReadFrom(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if st.minKey == nil {
			st.minKey = &node.key
		} else if node.key < *st.minKey {
			st.minKey = &node.key
		}

		if st.maxKey == nil {
			st.maxKey = &node.key
		} else if node.key > *st.maxKey {
			st.maxKey = &node.key
		}

		if i%keysPerIndex == 0 {
			st.keys = append(st.keys, node.key)
			st.keyToOffset[node.key] = offset
		}
		i++
	}

	sort.Strings(st.keys)
	return nil
}

func newSsTable(log log.LogInterface, filePath string, nodeMap map[string]*LsmNode) (*SsTable, error) {
	st := new(SsTable)
	st.filePath = filePath
	st.log = log
	file, err := os.OpenFile(st.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Pf(0, "Create table %s error %v", st.filePath, err)
		return nil, err
	}

	keys := make([]string, len(nodeMap))
	i := 0
	for key := range nodeMap {
		keys[i] = key
		i++
	}
	sort.Strings(keys)

	for _, key := range keys {
		node := nodeMap[key]
		err = node.WriteTo(file)
		if err != nil {
			file.Close()
			os.Remove(st.filePath)
			return nil, err
		}
	}

	/*
		err = file.Sync()
		if err != nil {
			file.Close()
			os.Remove(st.filePath)
			return nil, err
		}
	*/

	err = st.index()
	if err != nil {
		file.Close()
		os.Remove(st.filePath)
		return nil, err
	}
	st.file = file
	return st, nil
}

func openSsTable(log log.LogInterface, filePath string) (*SsTable, error) {
	st := new(SsTable)
	st.filePath = filePath
	st.log = log
	file, err := os.OpenFile(st.filePath, os.O_RDWR, 0600)
	if err != nil {
		log.Pf(0, "Open table %s error %v", st.filePath, err)
		return nil, err
	}
	st.file = file
	err = st.index()
	if err != nil {
		st.file.Close()
		return nil, err
	}
	return st, nil
}

func (st *SsTable) Get(key string) (string, error) {
	st.lock.RLock()
	defer st.lock.RUnlock()

	if st.minKey != nil && key < *st.minKey {
		return "", ErrNotFound
	}

	if st.maxKey != nil && key > *st.maxKey {
		return "", ErrNotFound
	}

	file, err := os.OpenFile(st.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()

	//st.log.Pf(0, "%s keys %d", st.filePath, len(st.keys))

	offset := int64(0)
	if len(st.keys) > 0 {
		keyIndex := sort.SearchStrings(st.keys, key)
		if keyIndex > 0 {
			keyIndex--
		}

		offset = st.keyToOffset[st.keys[keyIndex]]
		_, err = file.Seek(offset, os.SEEK_SET)
		if err != nil {
			return "", err
		}
	}

	for {
		offset, err = file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return "", err
		}

		//st.log.Pf(0, "lookup %s at %d for key %s", st.filePath, offset, key)
		node := new(LsmNode)
		err = node.ReadFrom(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		if node.key == key {
			if node.deleted {
				return "", ErrDeleted
			}

			return node.value, nil
		}
		if node.key > key {
			break
		}
	}

	return "", ErrNotFound
}

func (st *SsTable) Close() {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.file.Close()
	st.log.Pf(0, "close %s", st.filePath)
	st.file = nil
	st.filePath = ""
}

func (st *SsTable) Erase() {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.file.Close()
	st.log.Pf(0, "erase %s", st.filePath)
	os.Remove(st.filePath)
	st.file = nil
	st.filePath = ""
}

func (currSt *SsTable) Merge(prevSt *SsTable, tmpFilePath string) error {
	prevSt.lock.RLock()
	defer prevSt.lock.RUnlock()

	currSt.lock.RLock()
	defer currSt.lock.RUnlock()

	var prevFile, currFile, tmpFile *os.File
	var err error

	defer func() {
		if prevFile != nil {
			prevFile.Close()
		}
		if currFile != nil {
			currFile.Close()
		}

		if err != nil {
			if tmpFile != nil {
				tmpFile.Close()
				os.Remove(tmpFilePath)
			}
		} else {
			if tmpFile != nil {
				tmpFile.Close()
			}
		}
	}()

	prevFile, err = os.OpenFile(prevSt.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	currFile, err = os.OpenFile(currSt.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	tmpFile, err = os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}

	var prevNode, currNode, newNode *LsmNode

	for {
		if prevNode == nil && prevFile != nil {
			prevNode = new(LsmNode)
			err = prevNode.ReadFrom(prevFile)
			if err != nil {
				if err != io.EOF {
					return err
				}
				prevFile.Close()
				prevFile = nil
				prevNode = nil
				err = nil
			}
		}

		if currNode == nil && currFile != nil {
			currNode = new(LsmNode)
			err = currNode.ReadFrom(currFile)
			if err != nil {
				if err != io.EOF {
					return err
				}
				currFile.Close()
				currFile = nil
				currNode = nil
				err = nil
			}
		}

		if currNode == nil && prevNode == nil {
			break
		}

		if currNode != nil && prevNode == nil {
			newNode = currNode
			currNode = nil
		} else if prevNode != nil && currNode == nil {
			newNode = prevNode
			prevNode = nil
		} else {
			if prevNode.key == currNode.key {
				newNode = currNode
				currNode = nil
				prevNode = nil
			} else if prevNode.key < currNode.key {
				newNode = prevNode
				prevNode = nil
			} else {
				newNode = currNode
				currNode = nil
			}
		}

		err = newNode.WriteTo(tmpFile)
		if err != nil {
			return err
		}
	}

	/*
		err = tmpFile.Sync()
		if err != nil {
			return err
		}
	*/

	return nil
}
