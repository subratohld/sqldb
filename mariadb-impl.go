package sqldb

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql" // Import mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/subratohld/modules/db/retry"
)

type Pool struct {
	db              *sqlx.DB
	maxRetries      int
	retriesInterval time.Duration
	retryableErros  []string
}

func NewPool(dsn string, maxRetries, maxOpenConn, maxIdle int, connMaxLifeTime, retriesInterval time.Duration, retryableErros []string) (Sql, error) {
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConn)        // Default is 0 (unlimited)
	db.SetMaxIdleConns(maxIdle)            // Default is 2
	db.SetConnMaxLifetime(connMaxLifeTime) // If 0, connections are reused forever

	return &Pool{
		db:              db,
		maxRetries:      maxRetries,
		retriesInterval: retriesInterval,
		retryableErros:  retryableErros,
	}, err
}

func (p Pool) retry(fn retry.RetryFunc) error {
	return retry.Do(fn, p.maxRetries, p.retriesInterval, p.retryableErros)
}

func (p Pool) DB() *sqlx.DB {
	return p.db
}

func (p Pool) Ping() (err error) {
	p.retry(func(attempt int) error {
		err = p.db.Ping()
		return err
	})
	return err
}

func (p Pool) CreateTx() (sqlTx *sqlx.Tx, err error) {
	p.retry(func(attempt int) error {
		sqlTx, err = p.db.Beginx()
		return err
	})
	return
}

func (p Pool) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.Exec(query, args...)
		return err
	})
	return
}

func (p Pool) Query(query string, args ...interface{}) (res *sql.Rows, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.Query(query, args...)
		return err
	})
	return
}

func (p Pool) NamedExec(query string, arg interface{}) (res sql.Result, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.NamedExec(query, arg)
		return err
	})
	return
}

func (p Pool) NamedQuery(query string, arg interface{}) (res *sqlx.Rows, err error) {
	p.retry(func(attempt int) error {
		res, err = p.db.NamedQuery(query, arg)
		return err
	})
	return
}

func (p Pool) Select(dest interface{}, query string, args ...interface{}) (err error) {
	p.retry(func(attempt int) error {
		err = p.db.Select(dest, query, args...)
		return err
	})
	return
}

type Transaction struct {
	tx              *sqlx.Tx
	maxRetries      int
	retriesInterval time.Duration
	retryableErros  []string
}

func NewTransaction(tx *sqlx.Tx, maxRetries int, retriesInterval time.Duration, retryableErros []string) SqlTx {
	return Transaction{
		tx:              tx,
		maxRetries:      maxRetries,
		retriesInterval: retriesInterval,
		retryableErros:  retryableErros,
	}
}

func (t Transaction) retry(fn retry.RetryFunc) error {
	return retry.Do(fn, t.maxRetries, t.retriesInterval, t.retryableErros)
}

func (p Transaction) NamedExec(query string, arg interface{}) (res sql.Result, err error) {
	p.retry(func(attempt int) error {
		res, err = p.tx.NamedExec(query, arg)
		return err
	})
	return
}

func (p Transaction) NamedQuery(query string, arg interface{}) (res *sqlx.Rows, err error) {
	p.retry(func(attempt int) error {
		res, err = p.tx.NamedQuery(query, arg)
		return err
	})
	return
}

func (p Transaction) Commit() (err error) {
	p.retry(func(attempt int) error {
		err = p.tx.Commit()
		return err
	})
	return
}

func (p Transaction) Rollback() (err error) {
	p.retry(func(attempt int) error {
		err = p.tx.Rollback()
		return err
	})
	return
}
