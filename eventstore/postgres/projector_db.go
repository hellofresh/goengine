package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type (
	projectorDB struct {
		dbDSN     string
		db        *sql.DB
		dbChannel string

		minReconnectInterval time.Duration
		maxReconnectInterval time.Duration

		logger logrus.FieldLogger
	}

	projectorTrigger func(ctx context.Context, projectConn *sql.Conn, streamConn *sql.Conn, notification *eventStoreNotification) error

	// eventStoreNotification is a representation of the data provided by postgres notify
	eventStoreNotification struct {
		No          int64  `json:"no"`
		EventName   string `json:"event_name"`
		AggregateID string `json:"aggregate_id"`
	}
)

// Exec execute the provided callback
func (s *projectorDB) Exec(ctx context.Context, callback func(ctx context.Context, conn *sql.Conn) error) error {
	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	if err := s.dbOpen(); err != nil {
		return err
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to db close connection")
		}
	}()

	return callback(ctx, conn)
}

// Trigger execute the provided trigger
func (s *projectorDB) Trigger(ctx context.Context, trigger projectorTrigger) error {
	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	return s.runTrigger(ctx, trigger, nil)
}

// Listen start listening on the configures dbChannel and when a notification is received call the trigger
func (s *projectorDB) Listen(ctx context.Context, trigger projectorTrigger) error {
	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	// Open a database
	if err := s.dbOpen(); err != nil {
		return err
	}

	// Create the postgres listener
	listener := pq.NewListener(s.dbDSN, s.minReconnectInterval, s.maxReconnectInterval, s.listenerStateCallback)
	defer func() {
		if err := listener.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to close database listener")
		}
	}()

	// Start listening to postgres notifications
	if err := listener.Listen(s.dbChannel); err != nil {
		return err
	}
	for {
		select {
		case n := <-listener.Notify:
			if err := s.listenerNotificationCallback(ctx, trigger, n); err != nil {
				return err
			}
		case <-ctx.Done():
			s.logger.Debug("context closed stopping projection")
			return nil
		}
	}
}

// Close closes the projectorDB and releases it's resources
func (s *projectorDB) Close() error {
	return s.dbClose()
}

// dbOpen open an internal db connection pool if none exists
func (s *projectorDB) dbOpen() (err error) {
	if s.db == nil {
		s.logger.Debug("opening db connection")
		s.db, err = sql.Open("postgres", s.dbDSN)
	}

	return
}

// dbClose closes the internal db connection pool if one exists
func (s *projectorDB) dbClose() error {
	if s.db == nil {
		return nil
	}

	db := s.db
	s.db = nil

	return db.Close()
}

// listenerNotificationCallback a callback func that transforms the notification and calls the trigger
func (s *projectorDB) listenerNotificationCallback(ctx context.Context, trigger projectorTrigger, n *pq.Notification) error {
	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	// Add fields to the logger if possible
	logger := s.logger
	if n != nil {
		logger = logger.WithFields(logrus.Fields{
			"channel":    n.Channel,
			"data":       n.Extra,
			"process_id": n.BePid,
		})
	}

	// Unmarshal the notification
	notification, err := s.unmarshalNotification(n)
	if err != nil {
		logger.WithError(err).Warn("invalid notification")
	} else {
		logger.WithField("data", notification).Debug("received notification")
	}

	// Trigger the projection
	// TODO add sync support
	if err := s.runTrigger(ctx, trigger, notification); err != nil {
		if errors.Cause(err) == ErrProjectionFailedToLock {
			logger.WithError(err).Info("ignoring notification: the projection is already locked")
			return nil
		}

		return err
	}

	return nil
}

// runTrigger runs a specified trigger with a set if db connections and the provided notification
func (s *projectorDB) runTrigger(ctx context.Context, trigger projectorTrigger, notification *eventStoreNotification) error {
	if err := s.dbOpen(); err != nil {
		return err
	}

	projectConn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := projectConn.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to db close project connection")
		}
	}()

	streamConn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := streamConn.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to db close stream connection")
		}
	}()

	return trigger(ctx, streamConn, projectConn, notification)
}

// listenerStateCallback a callback used for getting state changes from a pq.Listener
// This callback will also close the related db connection pool used to query/persist projection data
func (s *projectorDB) listenerStateCallback(event pq.ListenerEventType, err error) {
	logger := s.logger.WithField("listener_event", event)
	if err != nil {
		logger = logger.WithError(err)
	}

	switch event {
	case pq.ListenerEventConnected:
		logger.Debug("connection listener: connected")
	case pq.ListenerEventConnectionAttemptFailed:
		logger.Debug("connection listener: failed to connect")
	case pq.ListenerEventDisconnected:
		logger.Debug("connection listener: disconnected")
		if err := s.dbClose(); err != nil {
			logger.WithError(err).Warn("failed to close related db connection")
		}
	case pq.ListenerEventReconnected:
		logger.Debug("connection listener: reconnected")
	default:
		logger.Warn("connection listener: unknown event")
	}
}

// unmarshalNotification takes a postgres notification and unmarshal it into a eventStoreNotification
func (*projectorDB) unmarshalNotification(n *pq.Notification) (*eventStoreNotification, error) {
	if n == nil {
		return nil, errors.New("nil notification")
	}

	if n.Extra == "" {
		return nil, errors.New("no notification data")
	}

	var notification *eventStoreNotification
	if err := json.Unmarshal([]byte(n.Extra), notification); err != nil {
		return nil, errors.Wrap(err, "invalid notification data")
	}

	return notification, nil
}
