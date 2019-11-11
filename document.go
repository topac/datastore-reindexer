package main

import (
	"cloud.google.com/go/datastore"
)

type document struct {
	ID *datastore.Key `datastore:"__key__"`
	// TODO: add your fields
}
