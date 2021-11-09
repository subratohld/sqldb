package samples

import (
	"fmt"
	"testing"

	"github.com/subratohld/sqldb"
)

func TestConnectivity(t *testing.T) {
	param := sqldb.Params{
		Username: "root",
		Database: "test-db",
		Host:     "localhost",
		Port:     "3306",
	}

	_, err := sqldb.DB(param)
	fmt.Println(err)
}
