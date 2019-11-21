package kvstore

import (
	"errors"
)

type KVStore interface {
	// Open database file.
	Open(path string) error

	// Close database file.
	Close()

	// Put a key-value pair into the bucket.
	// It is safe to modify the contents of the arguments after Put returns but not before.
	Put(bucket, key, value []byte) error

	// Get the value of this key from bucket.
	// The returned slice is its own copy, it is safe to modify the contents of the returned slice.
	Get(bucket, key []byte) ([]byte, error)

	// Delete the key in bucket.
	Delete(bucket, key []byte) error

	// Get all keys from bucket.
	// // The returned slice is its own copy, it is safe to modify the contents of the returned slice.
	AllKeys(bucket []byte) ([][]byte, error)

	// Sequence returns the current integer for the bucket without incrementing it.
	Sequence(bucket []byte) uint64

	// SetSequence updates the sequence number for the bucket.
	SetSequence(bucket []byte, sequence uint64) error

	// NextSequence returns an autoincrementing integer for the bucket.
	NextSequence(bucket []byte) (uint64, error)

	// Count return number of key-value pairs in bucket.
	Count(bucket []byte) uint64
}

var (
	ErrClosed   = errors.New("kvstore: closed")
	ErrNotFound = errors.New("kvstore: key not found")
)
