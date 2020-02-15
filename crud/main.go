package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	db, err := sql.Open("sqlite3", "crud.sqlite")
	if err != nil {
		return err
	}
	log.Printf("db: %v", db)
	return nil
}
