package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/innoq/go-sqlite-example/utils"
	_ "github.com/mattn/go-sqlite3"
)

var tweetsJSON = `[
	{
		"id": "123123123",
		"data": "2020-01-01 10:15:23",
		"author": "alice",
		"content": "hello world!", 
		"mentions":[]
	},
	{
		"id": "123123143",
		"data": "2020-01-01 06:15:23",
		"author": "alice",
		"content": "Hello?!", 
		"mentions":[]
	},
	{
		"id": "123123200",
		"data": "2020-01-08 08:12:23",
		"author": "bob",
		"content": "hi @alice!", 
		"mentions":["alice"]
	},
	{
		"id": "123122001",
		"data": "2020-01-03 15:23:23",
		"author": "hal",
		"content": "hi @alice, hello @bob!", 
		"mentions":["alice", "bob"]
	}
]`

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	utils.CleanDBFile("json.sqlite")
	db, err := sql.Open("sqlite3", "json.sqlite")
	if err != nil {
		return err
	}

	err = utils.SimpleTableSetup(db)
	if err != nil {
		return err
	}

	err = insertTweets(db, []byte(tweetsJSON))
	if err != nil {
		return err
	}

	tweets, err := queryCreated(db, "alice")
	if err != nil {
		return err
	}
	log.Printf("User '%s' authored %d tweets!", "alice", len(tweets))

	return nil
}

func insertTweets(db *sql.DB, payload []byte) error {
	var tweets []interface{}
	err := json.Unmarshal(payload, &tweets)
	if err != nil {
		return err
	}

	for _, entry := range tweets {
		tweet := entry.(map[string]interface{})
		err := insertTweet(db, tweet)
		if err != nil {
			return err
		}
	}
	return nil
}

// Tweet - one tweet
type Tweet map[string]interface{}

func queryCreated(db *sql.DB, user string) ([]Tweet, error) {
	tweets := make([]Tweet, 0)
	queryStmt := fmt.Sprintf(
		`SELECT content 
		FROM data 
		WHERE json_extract(content, '$.author')='%s'`, user)
	rows, err := db.Query(queryStmt)
	if err != nil {
		return tweets, err
	}
	for rows.Next() {
		var tweet Tweet
		var content string
		err = rows.Scan(&content)
		if err != nil {
			return tweets, err
		}
		err := json.Unmarshal([]byte(content), &tweet)
		if err != nil {
			return tweets, err
		}
		log.Printf("Result: %s", tweet)
		tweets = append(tweets, tweet)
	}
	log.Println("")
	return tweets, nil
}

func queryMentions(db *sql.DB, user string) ([]Tweet, error) {
	rows := make([]Tweet, 0)
	//queryStmt := `SELECT content FROM data WHERE json_extract(content, '$.%s')='%s'`
	return rows, nil
}

func insertTweet(db *sql.DB, tweet map[string]interface{}) error {

	sqlStmt := fmt.Sprintf(`INSERT INTO data 
		(id, content) 
		VALUES 
		(?, json(?))`)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()

	id, err := utils.GetID(tweet)
	if err != nil {
		return err
	}

	content, err := json.Marshal(tweet)
	if err != nil {
		return err
	}

	log.Printf("Inserting %s: %s", id, content)

	result, err := stmt.Exec(id, string(content))
	if err != nil {
		return err
	}
	utils.LogResult(result)

	tx.Commit()
	log.Println("")

	return nil
}
