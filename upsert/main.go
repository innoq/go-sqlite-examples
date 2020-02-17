package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/innoq/go-sqlite-example/utils"
	"github.com/lithammer/shortuuid"
	_ "github.com/mattn/go-sqlite3"
)

type entry struct {
	id      string
	content string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	utils.CleanDBFile("upsert.sqlite")
	db, err := sql.Open("sqlite3", "upsert.sqlite")
	if err != nil {
		return err
	}

	err = utils.SimpleTableSetup(db)
	if err != nil {
		return err
	}

	row := &entry{id: shortuuid.New(), content: "foo"}
	err = insertEntry(db, row)
	if err != nil {
		return err
	}

	err = insertEntry(db, row)
	if err != nil {
		log.Printf("error %s", err)
		log.Println("")
	}

	row2 := &entry{id: shortuuid.New(), content: "bar"}
	err = upsertEntry(db, row2)
	if err != nil {
		return err
	}

	err = upsertEntry(db, row2)
	if err != nil {
		return err
	}

	return nil
}

func insertEntry(db *sql.DB, row *entry) error {
	log.Println("INSERTING VALUES")
	log.Println("")
	sqlStmt := fmt.Sprintf(`INSERT INTO data 
		(id, content) 
		VALUES 
		(?, ?)`)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sqlStmt)
	defer tx.Commit()

	if err != nil {
		return err
	}
	defer stmt.Close()

	log.Printf("Inserting %s: %s", row.id, row.content)
	result, err := stmt.Exec(row.id, row.content)
	if err != nil {
		return err
	}
	utils.LogResult(result)

	log.Println("")
	return nil
}

func upsertEntry(db *sql.DB, row *entry) error {
	log.Println("UPSERTING VALUES")
	log.Println("")
	sqlStmt := fmt.Sprintf(`INSERT INTO data 
		(id, content) 
		VALUES 
		(?, ?) 
		ON CONFLICT (id) 
		DO UPDATE SET content=? 
		WHERE id = ?`)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sqlStmt)
	defer tx.Commit()

	if err != nil {
		return err
	}
	defer stmt.Close()

	log.Printf("Upserting %s: %s", row.id, row.content)
	result, err := stmt.Exec(
		row.id, row.content,
		row.content,
		row.id)
	if err != nil {
		return err
	}
	utils.LogResult(result)

	log.Println("")
	return nil
}
