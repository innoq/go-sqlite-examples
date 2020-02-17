package utils

import (
	"database/sql"
	"log"
	"os"
)

// LogResult - log the result stats and returns the id
func LogResult(result sql.Result) (int64, error) {
	id, err := result.LastInsertId()
	if err != nil {
		return id, err
	}
	numRows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	log.Printf("created table id %d, affecded rows: %d", id, numRows)
	return id, nil
}

// CleanDBFile - removes a previous created db
func CleanDBFile(database string) {
	var err = os.Remove(database)
	if err != nil {
		log.Printf("error while deleting old db: %s", err)
	}
}
