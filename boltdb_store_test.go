package kvstore

import (
	"fmt"
	"os"
	"testing"
)

func TestBoltDB(t *testing.T) {
	dbpath := "testdb.db"

	db := NewBoltDBStore()
	err := db.Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucketname := []byte("mybucket")

	db.Put(bucketname, []byte("key1"), []byte("value1"))
	db.Put(bucketname, []byte("key2"), []byte("value2"))
	db.Put(bucketname, []byte("key2"), []byte("value22222"))

	if db.Count(bucketname) != 2 {
		t.Fatal()
	}

	keys, err := db.AllKeys(bucketname)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatal()
	}
	for i := 0; i < len(keys); i++ {
		fmt.Println(string(keys[i]))
	}

	value1, err := db.Get(bucketname, []byte("key1"))
	if err != nil {
		t.Fatal(err)
	}

	if string(value1) != "value1" {
		t.Fatal("get value failed")
	}

	db.Delete(bucketname, []byte("key2"))
	_, err = db.Get(bucketname, []byte("key2"))
	if err == nil {
		t.Fatal("delete failed")
	}

	if db.Count(bucketname) != 1 {
		t.Fatal()
	}

	for i := 1; i <= 5; i++ {
		curSeq := db.Sequence(bucketname)
		seq, _ := db.NextSequence(bucketname)
		if seq != uint64(i) || seq != curSeq+1 {
			t.Fatal()
		}
	}

	os.Remove(dbpath)
}
