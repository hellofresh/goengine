package internal

import (
	"context"
	"database/sql"
	"database/sql/driver"

	goengine_dev "github.com/hellofresh/goengine-dev"
)

// AcquireConn will return a new connection.
// This is needed due to https://github.com/golang/go/issues/29684
func AcquireConn(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	for i := 0; i < 3; i++ {
		conn, err := db.Conn(ctx)
		if err == nil || err != driver.ErrBadConn {
			return conn, err
		}

		// We have a bad connection so we ping the server and try again
		if err := db.PingContext(ctx); err != nil {
			// Even new connections return an error so return the error
			return nil, err
		}
	}

	return nil, driver.ErrBadConn
}

// ExecOnConn execute the provided callback and provides it with a *sql.Conn
func ExecOnConn(
	ctx context.Context,
	db *sql.DB,
	logger goengine_dev.Logger,
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
