package retry

import (
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

type RetryFunc func(attempt int) error

func Do(fn RetryFunc, maxRetries int, interval time.Duration, retryableErrors []string) (err error) {
	attempt := 1
	for {
		err = fn(attempt)
		if err == nil || Errors(retryableErrors).IsRetryable(err) {
			break
		}

		attempt++
		time.Sleep(interval)
		if attempt > maxRetries {
			return errors.New("exceeded retry limit. " + err.Error())
		}
	}
	return
}

type Errors []string

func (errs Errors) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	if err == io.EOF {
		return true
	}

	for _, retryableErr := range errs {
		if retryableErr == "*" {
			return true
		}

		if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(retryableErr)) {
			return true
		}
	}

	_, ok := err.(net.Error)
	return ok
}
