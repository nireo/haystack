package main

import (
	"flag"
	"log"

	"github.com/nireo/haystack/store"
)

func main() {
	port := flag.Int("port", 8000, "the port on which to host the server")
	logicalVolumeDir := flag.String("dir", "./logicals", "the path to the logical volume files")
	flag.Parse()

	st, err := store.NewStore(*logicalVolumeDir)
	if err != nil {
		log.Fatalf("[FATAL] store: cannot create store instance: %s", err)
	}
	defer st.Shutdown()

	storeServer := store.NewStoreServer(st, *port)
	storeServer.Start()
}
