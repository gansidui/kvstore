package kvstore

import (
	"bytes"
	"errors"
	"log"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	// reserved key for count the number of keys in bucket.
	KeyForCount = []byte("__key_for_count__")

	// reserved key for sequence
	KeyForSequence = []byte("__key_for_sequence__")

	// errors
	ErrClosed   = errors.New("kvstore: closed")
	ErrNotFound = errors.New("kvstore: key not found")
)

type KVStore struct {
	db    *leveldb.DB
	mutex sync.Mutex
}

// Open the DB file
func (this *KVStore) Open(path string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	var err error
	this.db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		log.Fatalf("KVStore open failed: %v", err)
	}
	log.Println("KVStore open", path)

	return err
}

// Close the DB file
func (this *KVStore) Close() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db != nil {
		this.db.Close()
		this.db = nil
	}

	log.Println("KVStore closed")
}

// It is safe to modify the contents of the arguments after Put returns but not before.
func (this *KVStore) Put(bucket, key, value []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrClosed
	}

	if this.exist(bucket, key) {
		return this.db.Put(this.getStoreKey(bucket, key), value, nil)

	} else {
		count := this.count(bucket) + 1

		// use a batch job
		batch := new(leveldb.Batch)
		batch.Put(this.getStoreKey(bucket, key), value)
		batch.Put(this.getCountKey(bucket), []byte(strconv.FormatUint(count, 10)))

		return this.db.Write(batch, nil)
	}

	return nil
}

// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
func (this *KVStore) Get(bucket, key []byte) ([]byte, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return nil, ErrClosed
	}

	return this.db.Get(this.getStoreKey(bucket, key), nil)
}

// Delete the key in bucket
func (this *KVStore) Delete(bucket, key []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrClosed
	}

	if this.exist(bucket, key) {
		count := this.count(bucket) - 1

		// use a batch job
		batch := new(leveldb.Batch)
		batch.Delete(this.getStoreKey(bucket, key))
		batch.Put(this.getCountKey(bucket), []byte(strconv.FormatUint(count, 10)))

		return this.db.Write(batch, nil)
	}

	return ErrNotFound
}

// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
func (this *KVStore) AllKeys(bucket []byte) ([][]byte, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return nil, ErrClosed
	}

	keys := make([][]byte, 0)
	countKey := this.getCountKey(bucket)
	prefix := append(bucket, "_"...) // storeKey = bucket_key

	iter := this.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		if bytes.Equal(iter.Key(), countKey) {
			continue
		}

		if len(iter.Key()) > len(prefix) {
			key := make([]byte, len(iter.Key())-len(prefix))
			copy(key, iter.Key()[len(prefix):]) // trim prefix
			keys = append(keys, key)
		}
	}
	iter.Release()

	return keys, nil
}

// Sequence returns the current integer for the bucket without incrementing it.
func (this *KVStore) Sequence(bucket []byte) uint64 {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0
	}

	return this.sequence(bucket)
}

func (this *KVStore) sequence(bucket []byte) uint64 {
	data, err := this.db.Get(this.getSequenceKey(bucket), nil)
	if err != nil {
		return 0
	}

	sequence, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0
	}

	return sequence
}

// NextSequence returns an autoincrementing integer for the bucket.
func (this *KVStore) NextSequence(bucket []byte) (uint64, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0, ErrClosed
	}

	sequence := this.sequence(bucket) + 1
	err := this.db.Put(this.getSequenceKey(bucket), []byte(strconv.FormatUint(sequence, 10)), nil)

	return sequence, err
}

// SetSequence updates the sequence number for the bucket.
func (this *KVStore) SetSequence(bucket []byte, sequence uint64) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrClosed
	}

	return this.db.Put(this.getSequenceKey(bucket), []byte(strconv.FormatUint(sequence, 10)), nil)
}

// Count returns the number of keys in bucket
func (this *KVStore) Count(bucket []byte) uint64 {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0
	}

	return this.count(bucket)
}

func (this *KVStore) count(bucket []byte) uint64 {
	data, err := this.db.Get(this.getCountKey(bucket), nil)
	if err != nil {
		return 0
	}

	count, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0
	}

	return count
}

func (this *KVStore) exist(bucket, key []byte) bool {
	exist, _ := this.db.Has(this.getStoreKey(bucket, key), nil)
	return exist
}

func (this *KVStore) getStoreKey(bucket, key []byte) []byte {
	// storeKey = bucket_key
	storeKey := make([]byte, len(bucket)+1+len(key))
	copy(storeKey, bucket)
	copy(storeKey[len(bucket):], "_")
	copy(storeKey[len(bucket)+1:], key)

	return storeKey
}

func (this *KVStore) getCountKey(bucket []byte) []byte {
	return append(bucket, KeyForCount...)
}

func (this *KVStore) getSequenceKey(bucket []byte) []byte {
	return append(bucket, KeyForSequence...)
}
