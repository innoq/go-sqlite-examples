package utils

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
	log.Printf("LastInsertId %d, RowsAffected: %d", id, numRows)
	return id, nil
}

// CleanDBFile - removes a previous created db
func CleanDBFile(database string) {
	var err = os.Remove(database)
	if err != nil {
		log.Printf("error while deleting old db: %s", err)
	}
}

// SimpleTableSetup - setup a simple table
func SimpleTableSetup(db *sql.DB) error {
	log.Println("SIMPLE TABLE SETUP")
	log.Println("")
	sqlStmt := `CREATE TABLE data (id TEXT not null primary key, content TEXT);`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	sqlStmt = "CREATE UNIQUE INDEX idx_data_id ON data(id);"
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	log.Println("")

	return nil
}

// GetSha1Checksum - returns the sha1 checksum of the given string
func GetSha1Checksum(content string) string {
	bv := []byte(content)
	h := sha1.New()
	h.Write(bv)
	return hex.EncodeToString(h.Sum(nil))
}

// GetID - returns a ID for given Map
func GetID(entry map[string]interface{}) (string, error) {
	b, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}
	return GetSha1Checksum(string(b)), nil
}
