package main

import (
	"log"

	"github.com/davidsutts/burrowdb"
)

type newType struct {
	Str   string
	Num   int64 `burrowdb:"ID"`
	Float float64
}

func main() {
	db, err := burrowdb.NewDB(burrowdb.WithDir("store"))
	if err != nil {
		log.Fatalf("unable to get new DB: %v", err)
	}

	oldVar := newType{
		Str:   "string",
		Num:   123,
		Float: 3.14,
	}

	err = db.Put(oldVar)
	if err != nil {
		log.Fatalf("unable to put var: %v", err)
	}

	newVar := newType{}
	err = db.GetByID(&newVar, 123)
	if err != nil {
		log.Fatal("unable to get variable with ID")
	}

	log.Printf("Got var: %+v", newVar)
}
