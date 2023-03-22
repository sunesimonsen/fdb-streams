package streams

import "encoding/binary"

func encodeUInt64(n uint64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, n)
	return bs
}
