package sqldb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/subratohld/modules/db/retry"
	"go.uber.org/multierr"
)

var (
	onceDb          sync.Once
	dbPool          Sql
	UserName        *string
	Password        *string
	Server          *string
	DBName          *string
	DBDSN           *string
	MaxCap          *int
	MaxIdle         *int
	ConnMaxLifetime *int
	MaxRetries      *int
	RetriesInterval *int
	RetryableErrors []string
)

// Creates new if already not created
func DB() (pool Sql, err error) {
	var dsn string
	dsn, err = getDsn()
	if err != nil {
		return
	}

	if MaxCap == nil {
		maxCap := 10
		MaxCap = &maxCap
	}

	if MaxRetries == nil {
		maxRetries := 5
		MaxRetries = &maxRetries
	}

	if MaxIdle == nil {
		maxIdle := 5
		MaxIdle = &maxIdle
	}

	if RetriesInterval == nil {
		retriesInterval := 5 // value is in seconds
		RetriesInterval = &retriesInterval
	}

	if ConnMaxLifetime == nil {
		connMaxLifetime := 1200 // value is in seconds
		ConnMaxLifetime = &connMaxLifetime
	}

	var connMaxLifetime time.Duration = time.Duration(*ConnMaxLifetime) * time.Second
	var retriesInterval time.Duration = time.Duration(*RetriesInterval) * time.Second

	newPool := func(attempt int) error {
		var err error
		dbPool, err = NewPool(dsn, *MaxRetries, *MaxCap, *MaxIdle, connMaxLifetime, retriesInterval, RetryableErrors)
		pool = dbPool
		return err
	}

	onceDb.Do(func() {
		err = retry.Do(newPool, *MaxRetries, retriesInterval, RetryableErrors)
	})

	if err != nil {
		onceDb = sync.Once{}
	}

	return
}

// Creates new transaction object every time
func Tx() (tx SqlTx, err error) {
	db, err := DB()
	if err != nil {
		return
	}

	var retriesInterval time.Duration = time.Duration(*RetriesInterval) * time.Second

	sqlTx, err := db.CreateTx()
	tx = NewTransaction(sqlTx, *MaxRetries, retriesInterval, RetryableErrors)

	return
}

func getDsn() (dsn string, err error) {
	if DBDSN != nil && *DBDSN != "" {
		return *DBDSN, nil
	}

	if Server == nil || *Server == "" {
		err = multierr.Append(err, errors.New("db: server address is empty"))
	}

	if UserName == nil || *UserName == "" {
		err = multierr.Append(err, errors.New("db: username is empty"))
	}

	if Password == nil {
		err = multierr.Append(err, errors.New("db: password is empty"))
	}

	if DBName == nil || *DBName == "" {
		err = multierr.Append(err, errors.New("db: db name is empty"))
	}

	if len(multierr.Errors(err)) == 0 {
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", *UserName, *Password, *Server, *DBName)
	}

	return
}
