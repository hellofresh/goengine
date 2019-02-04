package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/eventstore/postgres"
	"github.com/hellofresh/goengine/projector"
	"github.com/hellofresh/goengine/projector/internal"
	"github.com/lib/pq"
)

var (
	// ErrEmptyDatabaseChannel occurs when an empty db channel was provided
	ErrEmptyDatabaseChannel = errors.New("empty db channel provided")
)

type listener struct {
	dbDSN     string
	dbChannel string

	minReconnectInterval time.Duration
	maxReconnectInterval time.Duration

	logger goengine_dev.Logger
}

func newListener(
	dbDSN string,
	dbChannel string,
	logger goengine_dev.Logger,
) (*listener, error) {
	switch {
	case strings.TrimSpace(dbDSN) == "":
		return nil, postgres.ErrNoDBConnect
	case strings.TrimSpace(dbChannel) == "":
		return nil, ErrEmptyDatabaseChannel
	}

	if logger == nil {
		logger = goengine_dev.NopLogger
	}

	return &listener{
		dbDSN:                dbDSN,
		dbChannel:            dbChannel,
		minReconnectInterval: time.Millisecond,
		maxReconnectInterval: time.Second,
		logger:               logger,
	}, nil
}

// Listen start listening on the configures dbChannel and when a notification is received call the trigger
func (s *listener) Listen(ctx context.Context, exec internal.Trigger) error {
	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
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

	// Execute an initial run of the projection.
	// This is done after db listen is started to avoid losing a set of messages while the listener creates a db connection.
	if err := exec(ctx, nil); err != nil {
		return err
	}

	for {
		select {
		case n := <-listener.Notify:
			// Unmarshal the notification
			notification := s.unmarshalNotification(n)

			// Execute the notification to be projected
			if err := exec(ctx, notification); err != nil {
				return err
			}
		case <-ctx.Done():
			s.logger.Debug("context closed stopping projection")
			return nil
		}
	}
}

// listenerStateCallback a callback used for getting state changes from a pq.Listener
// This callback will also close the related db connection pool used to query/persist projection data
func (s *listener) listenerStateCallback(event pq.ListenerEventType, err error) {
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
	case pq.ListenerEventReconnected:
		logger.Debug("connection listener: reconnected")
	default:
		logger.Warn("connection listener: unknown event")
	}
}

// unmarshalNotification takes a postgres notification and unmarshal it into a eventStoreNotification
func (s *listener) unmarshalNotification(n *pq.Notification) (notification *projector.Notification) {
	if n == nil {
		s.logger.Info("received nil notification")
		return
	}

	logger := s.logger.WithField("pq_notification", n)
	if n.Extra == "" {
		logger.Error("received notification without extra data")
		return
	}

	notification = &projector.Notification{}
	if err := json.Unmarshal([]byte(n.Extra), notification); err != nil {
		logger.WithError(err).Error("received invalid notification data")
		return nil
	}

	logger.WithField("notification", notification).Debug("received notification")

	return notification
}
