package kvstore

import (
	"log"
	"os"
	"sync"

	"github.com/boltdb/bolt"
)

type BoltDBStore struct {
	db    *bolt.DB
	mutex sync.Mutex
}

func NewBoltDBStore() KVStore {
	return &BoltDBStore{}
}

func (this *BoltDBStore) Open(path string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	var err error
	this.db, err = bolt.Open(path, os.ModePerm, nil)
	if err != nil {
		log.Fatalf("BoltDBStore open failed: %v", err)
	}
	log.Println("BoltDBStore open", path)

	return err
}

func (this *BoltDBStore) Close() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db != nil {
		this.db.Close()
		this.db = nil
	}

	log.Println("BoltDBStore closed")
}

func (this *BoltDBStore) Put(bucket, key, value []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrDBClosed
	}

	return this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			var err error
			if b, err = tx.CreateBucket(bucket); err != nil {
				return err
			}
		}
		return b.Put(key, value)
	})
}

func (this *BoltDBStore) Get(bucket, key []byte) ([]byte, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return nil, ErrDBClosed
	}

	value := make([]byte, 0)
	err := this.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}
		v := b.Get(key)
		if v == nil {
			return ErrKeyNotFound
		}

		if len(v) > 0 {
			value = make([]byte, len(v))
			copy(value, v)
		}

		return nil
	})
	return value, err
}

func (this *BoltDBStore) Delete(bucket, key []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrDBClosed
	}

	return this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			return b.Delete(key)
		}
		return nil
	})
}

func (this *BoltDBStore) AllKeys(bucket []byte) ([][]byte, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return nil, ErrDBClosed
	}

	keys := make([][]byte, 0)
	err := this.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}

		b.ForEach(func(k, v []byte) error {
			if len(k) > 0 {
				key := make([]byte, len(k))
				copy(key, k)
				keys = append(keys, key)
			}
			return nil
		})

		return nil
	})

	return keys, err
}

func (this *BoltDBStore) Sequence(bucket []byte) uint64 {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0
	}

	var seq uint64 = 0
	this.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			seq = b.Sequence()
		}
		return nil
	})

	return seq
}

func (this *BoltDBStore) NextSequence(bucket []byte) (uint64, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0, ErrDBClosed
	}

	var seq uint64
	err := this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			var err error
			if b, err = tx.CreateBucket(bucket); err != nil {
				return err
			}
		}

		seq2, err := b.NextSequence()
		if err == nil {
			seq = seq2
		}
		return err
	})

	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (this *BoltDBStore) SetSequence(bucket []byte, sequence uint64) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return ErrDBClosed
	}

	return this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}
		return b.SetSequence(sequence)
	})
}

func (this *BoltDBStore) Count(bucket []byte) uint64 {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.db == nil {
		return 0
	}

	keyN := 0
	this.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}

		stats := b.Stats()
		keyN = stats.KeyN // int

		return nil
	})

	return uint64(keyN)
}
