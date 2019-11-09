package kvstore

import (
	"fmt"
	"strconv"
	"testing"
)

func Test(t *testing.T) {
	path := "_test.db"

	db := &KVStore{}
	db.Open(path)
	defer db.Close()

	bucket := []byte("bucket1")
	db.Put(bucket, []byte("key1"), []byte("value1"))
	db.Put(bucket, []byte("key2"), []byte("value2"))
	db.Put(bucket, []byte("key3"), []byte("value3"))

	keys, _ := db.AllKeys(bucket)
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

func Test2(t *testing.T) {
	path := "_test.db"

	db := &KVStore{}
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
