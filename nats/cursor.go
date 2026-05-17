package nats

import "encoding/binary"

func cursorFromSeq(seq uint64) []byte {
	cursor := make([]byte, 8)
	binary.BigEndian.PutUint64(cursor, seq)
	return cursor
}

func cursorToSeq(cursor []byte) uint64 {
	if len(cursor) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(cursor)
}
