package sql

import (
	"context"
	"database/sql"
)

type (
	// Execer a interface used to execute a query on a sql.DB, sql.Conn or sql.Tx
	Execer interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}

	// Queryer an interface used to query a sql.DB, sql.Conn or sql.Tx
	Queryer interface {
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	}
)
