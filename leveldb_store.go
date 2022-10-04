package kvstore

import (
	"bytes"
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
)

type LevelDBStore struct {
	db    *leveldb.DB
	mutex sync.RWMutex
}

func NewLevelDBStore() KVStore {
	return &LevelDBStore{}
}

func (this *LevelDBStore) Open(path string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	var err error
	this.db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		log.Fatalf("LevelDBStore open failed: %v", err)
	}
	log.Println("LevelDBStore open", path)

	return err
}

func (this *LevelDBStore) Close() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db != nil {
		this.db.Close()
		this.db = nil
	}

	log.Println("LevelDBStore closed")
}

func (this *LevelDBStore) Put(bucket, key, value []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrDBClosed
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

func (this *LevelDBStore) Get(bucket, key []byte) ([]byte, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.db == nil {
		return nil, ErrDBClosed
	}

	return this.db.Get(this.getStoreKey(bucket, key), nil)
}

func (this *LevelDBStore) Delete(bucket, key []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrDBClosed
	}

	if this.exist(bucket, key) {
		count := this.count(bucket) - 1

		// use a batch job
		batch := new(leveldb.Batch)
		batch.Delete(this.getStoreKey(bucket, key))
		batch.Put(this.getCountKey(bucket), []byte(strconv.FormatUint(count, 10)))

		return this.db.Write(batch, nil)
	}

	return ErrKeyNotFound
}

func (this *LevelDBStore) AllKeys(bucket []byte) ([][]byte, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.db == nil {
		return nil, ErrDBClosed
	}

	// exclude keys
	countKey := this.getCountKey(bucket)
	sequenceKey := this.getSequenceKey(bucket)

	keys := make([][]byte, 0)
	prefix := append(bucket, "_"...) // storeKey = bucket_key

	iter := this.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		if bytes.Equal(iter.Key(), countKey) || bytes.Equal(iter.Key(), sequenceKey) {
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

func (this *LevelDBStore) Sequence(bucket []byte) uint64 {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.db == nil {
		return 0
	}

	return this.sequence(bucket)
}

func (this *LevelDBStore) sequence(bucket []byte) uint64 {
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

func (this *LevelDBStore) NextSequence(bucket []byte) (uint64, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0, ErrDBClosed
	}

	sequence := this.sequence(bucket) + 1
	err := this.db.Put(this.getSequenceKey(bucket), []byte(strconv.FormatUint(sequence, 10)), nil)

	return sequence, err
}

func (this *LevelDBStore) SetSequence(bucket []byte, sequence uint64) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrDBClosed
	}

	return this.db.Put(this.getSequenceKey(bucket), []byte(strconv.FormatUint(sequence, 10)), nil)
}

func (this *LevelDBStore) Count(bucket []byte) uint64 {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.db == nil {
		return 0
	}

	return this.count(bucket)
}

func (this *LevelDBStore) count(bucket []byte) uint64 {
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

func (this *LevelDBStore) exist(bucket, key []byte) bool {
	exist, _ := this.db.Has(this.getStoreKey(bucket, key), nil)
	return exist
}

func (this *LevelDBStore) getStoreKey(bucket, key []byte) []byte {
	// storeKey = bucket_key
	storeKey := make([]byte, len(bucket)+1+len(key))
	copy(storeKey, bucket)
	copy(storeKey[len(bucket):], "_")
	copy(storeKey[len(bucket)+1:], key)

	return storeKey
}

func (this *LevelDBStore) getCountKey(bucket []byte) []byte {
	return append(bucket, KeyForCount...)
}

func (this *LevelDBStore) getSequenceKey(bucket []byte) []byte {
	return append(bucket, KeyForSequence...)
}
