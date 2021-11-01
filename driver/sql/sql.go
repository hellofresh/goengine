package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
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

// AcquireConn will return a new connection
func AcquireConn(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	var (
		timeoutCtx    context.Context
		timeoutCancel context.CancelFunc
	)

	for i := 0; i < 3; i++ {
		// Use a context with a timeout this avoid hanging when the db.MaxOpenConnections is reached
		timeoutCtx, timeoutCancel = context.WithTimeout(ctx, time.Second)

		conn, err := db.Conn(timeoutCtx)
		timeoutCancel()

		switch err {
		case nil:
			return conn, nil
		case context.DeadlineExceeded:
			return nil, ErrConnFailedToAcquire
		case driver.ErrBadConn:
			// This is needed due to https://github.com/golang/go/issues/29684
			// We have a bad connection, so we ping the server and try again
			timeoutCtx, timeoutCancel = context.WithTimeout(ctx, time.Second)
			err := db.PingContext(timeoutCtx)
			timeoutCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					return nil, ErrConnFailedToAcquire
				}

				// Even new connections return an error so return the error
				return nil, err
			}
		default:
			return nil, err
		}
	}

	return nil, driver.ErrBadConn
}
