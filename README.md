# Google Datastore REindexer

Iterate over each document of the given *kind* and re-save them causing
the attributes (not tagged with *noindex*) to be properly indexed.

Fill the *document* `struct` first!
```go
type document struct {
	// Add your fields
}
```

Usage example:

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/legacy_credentials/foobar/adc.json
$ export DATASTORE_PROJECT_ID=xyz
$ go run ./... -kind myKind

INFO[0000] counting...
INFO[0045] total documents: 1479648
INFO[0045] progress: 0 of 1479648
INFO[0051] progress: 10000 of 1479648
INFO[0057] progress: 20000 of 1479648
...
```
