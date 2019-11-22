Installation
---------------

	go get github.com/gansidui/kvstore


Usage
---------------


~~~Go

package main

import (
	"log"

	"github.com/gansidui/kvstore"
)

func main() {
	db := kvstore.NewBoltDBStore()
	// db := kvstore.NewLevelDBStore()

	// open
	err := db.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// put
	db.Put([]byte("bucket"), []byte("key"), []byte("value"))

	// get
	if value, err := db.Get([]byte("bucket"), []byte("key")); err == nil {
		log.Println(string(value))
	}
}

~~~

