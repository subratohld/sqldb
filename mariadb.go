package sqldb

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql" // Import mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/subratohld/retry"
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

type database struct {
	db              *sqlx.DB
	maxRetries      int
	retriesInterval time.Duration
	retryableErros  []string
}

func newDB(dsn string, maxRetries, maxOpenConn, maxIdle int, connMaxLifeTime, retriesInterval time.Duration, retryableErros []string) (Sql, error) {
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConn)        // Default is 0 (unlimited)
	db.SetMaxIdleConns(maxIdle)            // Default is 2
	db.SetConnMaxLifetime(connMaxLifeTime) // If 0, connections are reused forever

	return &database{
		db:              db,
		maxRetries:      maxRetries,
		retriesInterval: retriesInterval,
		retryableErros:  retryableErros,
	}, err
}

func (p database) retry(fn retry.RetryFunc) error {
	return retry.Do(fn, p.maxRetries, p.retriesInterval, p.retryableErros)
}

func (p database) DB() *sqlx.DB {
	return p.db
}

func (p database) Ping() (err error) {
	p.retry(func(attempt int) error {
		err = p.db.Ping()
		return err
	})
	return err
}

func (p database) CreateTx() (sqlTx *sqlx.Tx, err error) {
	p.retry(func(attempt int) error {
		sqlTx, err = p.db.Beginx()
		return err
	})
	return
}

func (p database) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.Exec(query, args...)
		return err
	})
	return
}

func (p database) Query(query string, args ...interface{}) (res *sql.Rows, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.Query(query, args...)
		return err
	})
	return
}

func (p database) NamedExec(query string, arg interface{}) (res sql.Result, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.NamedExec(query, arg)
		return err
	})
	return
}

func (p database) NamedQuery(query string, arg interface{}) (res *sqlx.Rows, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.NamedQuery(query, arg)
		return err
	})
	return
}

func (p database) Select(dest interface{}, query string, args ...interface{}) (err error) {
	p.retry(func(attempt int) error {
		err = p.db.Select(dest, query, args...)
		return err
	})
	return
}

// For transaction management

type SqlTx interface {
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	Commit() error
	Rollback() error
}

type transaction struct {
	tx              *sqlx.Tx
	maxRetries      int
	retriesInterval time.Duration
	retryableErros  []string
}

func newTransaction(tx *sqlx.Tx, maxRetries int, retriesInterval time.Duration, retryableErros []string) SqlTx {
	return transaction{
		tx:              tx,
		maxRetries:      maxRetries,
		retriesInterval: retriesInterval,
		retryableErros:  retryableErros,
	}
}

func (t transaction) retry(fn retry.RetryFunc) error {
	return retry.Do(fn, t.maxRetries, t.retriesInterval, t.retryableErros)
}

func (p transaction) NamedExec(query string, arg interface{}) (res sql.Result, err error) {
	p.retry(func(attempt int) error {
		res, err = p.tx.NamedExec(query, arg)
		return err
	})
	return
}

func (p transaction) NamedQuery(query string, arg interface{}) (res *sqlx.Rows, err error) {
	p.retry(func(attempt int) error {
		res, err = p.tx.NamedQuery(query, arg)
		return err
	})
	return
}

func (p transaction) Commit() (err error) {
	p.retry(func(attempt int) error {
		err = p.tx.Commit()
		return err
	})
	return
}

func (p transaction) Rollback() (err error) {
	p.retry(func(attempt int) error {
		err = p.tx.Rollback()
		return err
	})
	return
}
