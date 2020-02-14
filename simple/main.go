package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	db, err := sql.Open("sqlite3", "simple.sqlite")
	if err != nil {
		return err
	}
	table := "foo"
	sqlStmt := fmt.Sprintf(`CREATE TABLE %s 
		(id TEXT not null primary key, 
		owner TEXT, 
		content TEXT);`, table)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	log.Printf("created table %s\n", table)

	stmt, err := db.Prepare(`SELECT name 
		FROM sqlite_master 
		WHERE type='table' AND name=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var name string
	err = stmt.QueryRow(table).Scan(&name)
	if err != nil {
		return err
	}

	log.Printf("found table %s\n", name)

	return nil
}
