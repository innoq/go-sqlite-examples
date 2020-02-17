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

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	utils.CleanDBFile("crud.sqlite")
	db, err := sql.Open("sqlite3", "crud.sqlite")
	if err != nil {
		return err
	}

	err = tableSetup(db)
	if err != nil {
		return err
	}

	err = insertValues(db)
	if err != nil {
		return err
	}
	countValues(db, "")
	countValues(db, "WHERE id > 4")

	err = selectValues(db, "")
	if err != nil {
		return err
	}

	err = selectValues(db, "WHERE id > 5")
	if err != nil {
		return err
	}

	countValues(db, "")
	err = deleteValues(db, "WHERE id > 5")
	if err != nil {
		return err
	}
	countValues(db, "")

	return nil
}

func tableSetup(db *sql.DB) error {
	log.Println("TABLE SETUP")
	log.Println("")
	sqlStmt := `CREATE TABLE data (id TEXT not null primary key, content TEXT);`
	result, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	_, err = utils.LogResult(result)
	if err != nil {
		return err
	}
	log.Println("")

	return nil
}

func insertValues(db *sql.DB) error {
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
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 1; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		content := fmt.Sprintf("content #%d %s", i, shortuuid.New())
		log.Printf("Inserting %s: %s", id, content)
		result, err := stmt.Exec(id, content)
		if err != nil {
			return err
		}
		utils.LogResult(result)
	}
	tx.Commit()
	log.Println("")

	return nil
}

func deleteValues(db *sql.DB, query string) error {
	log.Println("DELETING VALUES")
	log.Println("")
	log.Printf("deleting values: %s", query)
	queryStmt := fmt.Sprintf("DELETE FROM data %s ", query)
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	result, err := db.Exec(queryStmt)
	if err != nil {
		return err
	}
	tx.Commit()
	utils.LogResult(result)
	log.Println("")

	return nil
}

type entry struct {
	id      string
	content string
}

func selectValues(db *sql.DB, query string) error {
	log.Printf("SELECT VALUES for '%s'", query)
	log.Println("")
	queryStmt := fmt.Sprintf("SELECT id, content FROM data %s ", query)
	rows, err := db.Query(queryStmt)
	if err != nil {
		return err
	}
	for rows.Next() {
		var row entry
		err = rows.Scan(&row.id, &row.content)
		if err != nil {
			return err
		}
		log.Printf("Result: %s", row)
	}
	log.Println("")

	return nil
}

func countValues(db *sql.DB, query string) error {
	queryStmt := fmt.Sprintf("SELECT count(id) as count FROM data %s ", query)
	rows, err := db.Query(queryStmt)
	if err != nil {
		return err
	}
	for rows.Next() {
		var count int
		err = rows.Scan(&count)
		if err == nil {
			log.Printf("Results for query '%s': %d", query, count)
		}
	}
	rows.Close()
	log.Println("")

	return nil
}
