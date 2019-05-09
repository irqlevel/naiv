package lsm

import (
	"bytes"
	"unsafe"

	"github.com/OneOfOne/xxhash"

	"encoding/binary"
	"fmt"
	"io"
)

var (
	ErrLsmNodeBadMagic    = fmt.Errorf("Lsm node bad magic")
	ErrLsmNodeBadCheckSum = fmt.Errorf("Lsm node bad checksum")
)

const (
	LsmNodeMagic = uint32(0x4CBDABDA)
	IoBlockSize  = 512
)

type LsmNode struct {
	key     string
	value   string
	deleted bool
}

func newLsmNode(key string, value string) *LsmNode {
	node := new(LsmNode)
	node.key = key
	node.value = value
	node.deleted = false
	return node
}

func getAlignment(block []byte, alignSize int) int {
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(alignSize-1))
}

func getAlignedBlock(blockSize int, alignSize int) []byte {
	block := make([]byte, blockSize+alignSize)
	if alignSize == 0 {
		return block
	}
	a := getAlignment(block, alignSize)
	offset := 0
	if a != 0 {
		offset = alignSize - a
	}
	block = block[offset : offset+blockSize]
	// Can't check alignment of a zero sized block
	if blockSize != 0 {
		a = getAlignment(block, alignSize)
		if a != 0 {
			panic("Failed to align block")
		}
	}
	return block
}

func getCopiedAlignedBlock(src []byte, alignSize int) []byte {
	blockSize := len(src)
	if len(src)%alignSize != 0 {
		blockSize = ((len(src) / alignSize) + 1) * alignSize
	}

	dst := getAlignedBlock(blockSize, alignSize)
	copy(dst, src)
	return dst
}

func getAlignedBlockByLen(srcLen int, alignSize int) []byte {
	blockSize := srcLen
	if srcLen%alignSize != 0 {
		blockSize = ((srcLen / alignSize) + 1) * alignSize
	}

	return getAlignedBlock(blockSize, alignSize)
}

func (node *LsmNode) WriteTo(f io.Writer) error {
	key := []byte(node.key)
	value := []byte(node.value)
	deleted := uint32(0)
	if node.deleted {
		deleted = 1
	}

	header := make([]byte, 16+8)
	binary.LittleEndian.PutUint32(header[0:], LsmNodeMagic)
	binary.LittleEndian.PutUint32(header[4:], deleted)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[12:], uint32(len(value)))

	h := xxhash.New64()
	h.Write(header[0:16])
	h.Write(key)
	h.Write(value)
	copy(header[16:16+8], h.Sum(nil))

	_, err := f.Write(getCopiedAlignedBlock(header, IoBlockSize))
	if err != nil {
		return err
	}

	_, err = f.Write(getCopiedAlignedBlock(key, IoBlockSize))
	if err != nil {
		return err
	}

	_, err = f.Write(getCopiedAlignedBlock(value, IoBlockSize))
	return err
}

func (node *LsmNode) ReadFrom(f io.Reader) error {
	header := getAlignedBlockByLen(16+8, IoBlockSize)
	_, err := f.Read(header)
	if err != nil {
		return err
	}

	if binary.LittleEndian.Uint32(header[0:]) != LsmNodeMagic {
		return ErrLsmNodeBadMagic
	}

	keyLength := int(binary.LittleEndian.Uint32(header[8:]))
	valueLength := int(binary.LittleEndian.Uint32(header[12:]))

	key := getAlignedBlockByLen(keyLength, IoBlockSize)
	value := getAlignedBlockByLen(valueLength, IoBlockSize)
	_, err = f.Read(key)
	if err != nil {
		return err
	}
	_, err = f.Read(value)
	if err != nil {
		return err
	}

	h := xxhash.New64()
	h.Write(header[0:16])
	h.Write(key[0:keyLength])
	h.Write(value[0:valueLength])

	if !bytes.Equal(header[16:16+8], h.Sum(nil)) {
		return ErrLsmNodeBadCheckSum
	}

	node.key = string(key[0:keyLength])
	node.value = string(value[0:valueLength])
	node.deleted = false
	if binary.LittleEndian.Uint32(header[4:]) != 0 {
		node.deleted = true
	}

	return nil
}
