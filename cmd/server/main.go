package main

import (
	"log"

	"github.com/wal99d/prolog/server"
)

func main() {
	srv := server.NewHttpServer(":8282")
	log.Fatal(srv.ListenAndServe())
}
