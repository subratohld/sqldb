package sqldb

import (
	"errors"
	"fmt"
	"time"

	"github.com/subratohld/retry"
	"go.uber.org/multierr"
)

type Params struct {
	DSN             string
	Host            string
	Port            string
	Username        string
	Password        string
	Database        string
	MaxCap          *int
	MaxIdle         *int
	ConnMaxLifetime *int
	MaxRetries      *int
	RetriesInterval *int
	RetryableErrors []string
}

func DB(param Params) (db Sql, err error) {
	var dsn string
	dsn, err = getDsn(&param)
	if err != nil {
		return
	}

	if param.MaxCap == nil {
		maxCap := 10
		param.MaxCap = &maxCap
	}

	if param.MaxRetries == nil {
		maxRetries := 5
		param.MaxRetries = &maxRetries
	}

	if param.MaxIdle == nil {
		maxIdle := 5
		param.MaxIdle = &maxIdle
	}

	if param.RetriesInterval == nil {
		retriesInterval := 5 // value is in seconds
		param.RetriesInterval = &retriesInterval
	}

	if param.ConnMaxLifetime == nil {
		connMaxLifetime := 1200 // value is in seconds
		param.ConnMaxLifetime = &connMaxLifetime
	}

	var connMaxLifetime time.Duration = time.Duration(*param.ConnMaxLifetime) * time.Second
	var retriesInterval time.Duration = time.Duration(*param.RetriesInterval) * time.Second

	dbConn := func(attempt int) error {
		var err error
		db, err = newDB(dsn, *param.MaxRetries, *param.MaxCap, *param.MaxIdle, connMaxLifetime, retriesInterval, param.RetryableErrors)
		return err
	}

	err = retry.Do(dbConn, *param.MaxRetries, retriesInterval, param.RetryableErrors)

	return
}

// Creates new transaction object every time
func Tx(param Params) (tx SqlTx, err error) {
	db, err := DB(param)
	if err != nil {
		return
	}

	var retriesInterval time.Duration = time.Duration(*param.RetriesInterval) * time.Second

	sqlTx, err := db.CreateTx()
	tx = newTransaction(sqlTx, *param.MaxRetries, retriesInterval, param.RetryableErrors)

	return
}

func getDsn(param *Params) (dsn string, err error) {
	if param.DSN != "" {
		return param.DSN, nil
	}

	if param.Host == "" {
		err = multierr.Append(err, errors.New("sqldb: host name is empty"))
	}

	if param.Port == "" {
		err = multierr.Append(err, errors.New("sqldb: port is empty"))
	}

	if param.Username == "" {
		err = multierr.Append(err, errors.New("sqldb: username is empty"))
	}

	if param.Database == "" {
		err = multierr.Append(err, errors.New("sqldb: database name is empty"))
	}

	if len(multierr.Errors(err)) == 0 {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			param.Username, param.Password, param.Host, param.Port, param.Database)
	}

	return
}
