package sqldb

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type Sql interface {
	DB() *sqlx.DB
	Ping() error
	CreateTx() (*sqlx.Tx, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, query string, args ...interface{}) error
}

type SqlTx interface {
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	Commit() error
	Rollback() error
}
