package main

import (
	"flag"
	"fmt"
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

	storeServer, err := store.NewHTTPService(fmt.Sprintf(":%d", *port), st, nil)
	if err != nil {
		log.Fatalf("[FATAL] store: cannot start http service: %s", err)
	}
	storeServer.Start()
}
