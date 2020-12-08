/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// Buffer provides a wrapper on top of proto.Buffer.
// The purpose of this wrapper is to get to know the current position in the []byte
type Buffer struct {
	buf      *proto.Buffer
	position int
}

// newBuffer constructs a new instance of Buffer
func NewBuffer(b []byte) *Buffer {
	return &Buffer{proto.NewBuffer(b), 0}
}

// DecodeVarint wraps the actual method and updates the position
func (b *Buffer) DecodeVarint() (uint64, error) {
	val, err := b.buf.DecodeVarint()
	if err == nil {
		b.position += proto.SizeVarint(val)
	} else {
		err = errors.Wrap(err, "error decoding varint with proto.Buffer")
	}
	return val, err
}

// DecodeRawBytes wraps the actual method and updates the position
func (b *Buffer) DecodeRawBytes(alloc bool) ([]byte, error) {
	val, err := b.buf.DecodeRawBytes(alloc)
	if err == nil {
		b.position += proto.SizeVarint(uint64(len(val))) + len(val)
	} else {
		err = errors.Wrap(err, "error decoding raw bytes with proto.Buffer")
	}
	return val, err
}

// GetBytesConsumed returns the offset of the current position in the underlying []byte
func (b *Buffer) GetBytesConsumed() int {
	return b.position
}
