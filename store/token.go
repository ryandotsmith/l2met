package store

import (
	"fmt"
	"l2met/db"
)

type Token struct {
	Id   string
	User string
	Pass string
}

func (t *Token) Get() {
	db.PGRLocker.Lock()
	defer db.PGRLocker.Unlock()
	rows, err := db.PGR.Query("select u, p from tokens where id = $1", t.Id)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		return
	}
	rows.Next()
	rows.Scan(&t.User, &t.Pass)
	rows.Close()
	return
}
