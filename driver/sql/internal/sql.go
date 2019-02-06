package internal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
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
			return nil, driverSQL.ErrConnFailedToAcquire
		case driver.ErrBadConn:
			// This is needed due to https://github.com/golang/go/issues/29684
			// We have a bad connection so we ping the server and try again
			timeoutCtx, timeoutCancel = context.WithTimeout(ctx, time.Second)
			err := db.PingContext(timeoutCtx)
			timeoutCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					return nil, driverSQL.ErrConnFailedToAcquire
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

// ExecOnConn execute the provided callback and provides it with a *sql.Conn
func ExecOnConn(
	ctx context.Context,
	db *sql.DB,
	logger goengine.Logger,
	callback func(ctx context.Context, conn *sql.Conn) error,
) error {
	conn, err := AcquireConn(ctx, db)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logger.WithError(err).Warn("failed to db close connection")
		}
	}()

	return callback(ctx, conn)
}
