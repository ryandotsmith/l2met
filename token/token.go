package token

import (
	"errors"
)

type Token struct {
	Id   string
	User string
	Pass string
}

func (t *Token) Get() error {
	rows, err := pg.Query("select u, p from tokens where id = $1", t.Id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&t.User, &t.Pass); err != nil {
			return err
		}
		return nil
	}
	return errors.New("Unable to find token")
}
