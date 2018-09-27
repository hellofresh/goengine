// +build unit

package test

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// RunWithMockDB runs f as a subtest of t called name and provided a mock database
func RunWithMockDB(t *testing.T, name string, f func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock)) {
	t.Run(name, func(t *testing.T) {
		db, dbMock, err := sqlmock.New()
		if !assert.NoError(t, err) {
			return
		}
		defer db.Close()

		f(t, db, dbMock)
	})
}
