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

func putDocument(client *datastore.Client, doc document) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Put(ctx, doc.ID, &doc)

	if err != nil {
		log.Fatalf("failed to put entity: %v", err)
	}
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
	entityNamePt := flag.String("kind", "", "Datastore kind")
	skipCountPt := flag.Bool("skipCount", false, "Skip initial count")
	allowAttributeDeletionPt := flag.Bool("allowAttributeDeletion", false, "Allow writing a document with less fields than the original one")
	emitProgressEveryPt := flag.Uint64("emitProgressEvery", 1000, "Display progress every N records")
	flag.Parse()

	dsKind := *entityNamePt
	allowAttributeDeletion := *allowAttributeDeletionPt

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
	if !(*skipCountPt) {
		log.Infof("counting...")
		total, err = client.Count(context.Background(), query)
		if err != nil {
			log.Fatalf("failed to count")
		}

		log.Infof("total documents: %d", total)
	}

	// spawn write workers
	var wg sync.WaitGroup
	var updated uint64
	numWorkers := 10 // arbitrary value
	chanSize := 250  // arbitrary value
	if total <= chanSize {
		chanSize = 0
	}
	docs := make(chan document, chanSize)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			for d := range docs {
				putDocument(client, d)

				if updated%(*emitProgressEveryPt) == 0 {
					displayProgress(int(updated), total)
				}
				atomic.AddUint64(&updated, 1)
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

		if err != nil && allowAttributeDeletion && strings.Contains(err.Error(), "no such struct field") {
			continue
		}

		if err != nil {
			log.Fatal(err)
		}

		docs <- doc
	}
}
