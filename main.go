package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/datastore"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func newDatastoreClient(ctx context.Context) *datastore.Client {
	projectID := os.Getenv("DATASTORE_PROJECT_ID")

	if len(projectID) == 0 {
		log.Fatal("DATASTORE_PROJECT_ID env variable is empty")
	}

	client, err := datastore.NewClient(
		ctx,
		projectID,
		option.WithGRPCDialOption(grpc.WithBackoffMaxDelay(5*time.Second)),
		option.WithGRPCDialOption(grpc.WithBlock()),
		option.WithGRPCDialOption(grpc.WithTimeout(10*time.Second)),
	)

	if err != nil {
		log.Fatalf("cannot create Datastore client for project %s: %v", projectID, err)
	}

	return client
}

func putDocument(client *datastore.Client, doc document) error {
	var err error

	for i := 0; i < 6; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_, err := client.Put(ctx, doc.ID, &doc)
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+3) * time.Second)
	}

	return err
}

func displayProgress(updated, total int) {
	if total <= 0 {
		log.Infof("progress: %d", updated)
	} else {
		log.Infof("progress: %d of %d (%d%%)", updated, total, (100 * int(updated) / total))
	}
}

func main() {
	// read args
	var skip, emitProgressEvery uint64
	var dsKind string
	var allowAttributeDeletion, skipCount bool
	var numWorkers int

	flag.StringVar(&dsKind, "kind", "", "Datastore kind")
	flag.BoolVar(&allowAttributeDeletion, "allowAttributeDeletion", false, "Allow writing a document with less fields than the original one")
	flag.BoolVar(&skipCount, "skipCount", false, "Skip initial count")
	flag.IntVar(&numWorkers, "workers", 4, "The number of workers to use")
	flag.Uint64Var(&skip, "skip", 0, "skip the first N documents")
	flag.Uint64Var(&emitProgressEvery, "emitProgressEvery", 1000, "Display progress every N records")

	flag.Parse()

	if dsKind == "" {
		flag.Usage()
		os.Exit(0)
	}

	// connect to google Datastore
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := newDatastoreClient(ctx)
	defer client.Close()

	// the main query
	query := datastore.NewQuery(dsKind)

	// count
	var total int
	var err error
	if !skipCount {
		log.Infof("main: count started, kind=%s", dsKind)
		total, err = client.Count(context.Background(), query)
		if err != nil {
			log.Fatalf("main: count failed: %v", err)
		}

		log.Infof("main: count completed, total=%d", total)
	}

	// spawn write workers
	var wg sync.WaitGroup
	var updated uint64
	chanSize := numWorkers * 50 // arbitrary value
	if total <= chanSize {
		chanSize = 0
	}
	docs := make(chan document, chanSize)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			for d := range docs {
				currentIndex := atomic.AddUint64(&updated, 1)

				if currentIndex >= skip {
					if err := putDocument(client, d); err != nil {
						log.Fatalf("main: putDocument failed, counter=%d, err=%v", updated, err)
					}
				}

				if updated%emitProgressEvery == 0 {
					displayProgress(int(updated), total)
				}
			}
			wg.Done()
		}()
	}

	// run the main query, iterate all the docs and push them in the docs channel
	it := client.Run(context.Background(), query)

	for {
		var doc document
		_, err := it.Next(&doc)

		if err == iterator.Done {
			break
		}

		noStructErr := err != nil && allowAttributeDeletion && strings.Contains(err.Error(), "no such struct field")

		if err != nil && !noStructErr {
			log.Fatal(err)
		}

		docs <- doc
	}

	close(docs)
	wg.Wait()
	log.Infof("main: done")
}
