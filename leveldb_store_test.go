package kvstore

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestLevelDB(t *testing.T) {
	path := "_test.db"

	db := NewLevelDBStore()
	db.Open(path)
	defer db.Close()

	bucket := []byte("bucket1")
	db.Put(bucket, []byte("key1"), []byte("value1"))
	db.Put(bucket, []byte("key2"), []byte("value2"))
	db.Put(bucket, []byte("key3"), []byte("value3"))

	if nextSeq, err := db.NextSequence(bucket); nextSeq != 1 || err != nil {
		t.Fatal()
	}

	keys, _ := db.AllKeys(bucket)
	if len(keys) != 3 {
		t.Fatal()
	}

	for _, key := range keys {
		value, err := db.Get(bucket, key)
		if err == nil {
			fmt.Println(string(key), string(value))
		} else {
			fmt.Println("err:", err, string(key))
		}
	}

	if db.Count(bucket) != 3 {
		t.Fatal()
	}

	if value, err := db.Get(bucket, []byte("key1")); string(value) != "value1" || err != nil {
		t.Fatal()
	}

	db.Delete(bucket, []byte("key1"))

	if db.Count(bucket) != 2 {
		t.Fatal()
	}

	_, err := db.Get(bucket, []byte("key1"))
	if err == nil {
		t.Fatal()
	}

	bucket2 := []byte("bucket2")
	db.Put(bucket2, []byte("k1"), []byte("v1"))
	db.Put(bucket2, []byte("k2"), []byte("v2"))
	db.Put(bucket2, []byte("k3"), []byte("v3"))

	keys, _ = db.AllKeys(bucket2)
	if string(keys[0]) != "k1" {
		t.Fatal()
	}

	fmt.Println("keys[0]", string(keys[0]))

	for _, key := range keys {
		value, err := db.Get(bucket2, key)
		if err == nil {
			fmt.Println(string(key), string(value))
		}
	}

	if value, err := db.Get(bucket2, keys[0]); string(value) != "v1" || err != nil {
		fmt.Println("ddd", string(value), err)
		t.Fatal()
	}

}

func TestLevelDB2(t *testing.T) {
	path := "_test.db"

	db := NewLevelDBStore()
	db.Open(path)
	defer db.Close()

	bucket := []byte("bucket")
	n := 1000 * 100
	for i := 0; i < n; i++ {
		db.Put(bucket, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}

	fmt.Println("count keys", db.Count(bucket))

	for i := 0; i < n; i++ {
		value, _ := db.Get(bucket, []byte(strconv.Itoa(i)))
		if string(value) != strconv.Itoa(i) {
			t.Fatal()
		}
	}
}

func TestLevelDBSequence(t *testing.T) {
	path := "_test.db"
	db := NewLevelDBStore()
	db.Open(path)
	defer db.Close()

	defer os.RemoveAll(path)

	bucket := []byte("bucket_seq")
	if db.Sequence(bucket) != 0 {
		t.Fatal()
	}

	for i := 1; i <= 1000; i++ {
		if seq, err := db.NextSequence(bucket); seq != uint64(i) || err != nil {
			t.Fatal()
		}
	}

	db.Put(bucket, []byte("key"), []byte("value"))
	if db.Sequence(bucket) != 1000 {
		t.Fatal()
	}

	if err := db.SetSequence(bucket, 2000); err != nil || db.Sequence(bucket) != 2000 {
		t.Fatal()
	}
	if seq, err := db.NextSequence(bucket); seq != 2001 || err != nil {
		t.Fatal()
	}

	if db.Sequence([]byte("bucket_bb")) != 0 {
		t.Fatal()
	}

	if seq, err := db.NextSequence([]byte("bucket_cc")); seq != 1 || err != nil {
		t.Fatal()
	}
}
