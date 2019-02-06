package pq

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"

	"github.com/lib/pq"
)

// Listener a Notification listener for pq
type Listener struct {
	dbDSN     string
	dbChannel string

	minReconnectInterval time.Duration
	maxReconnectInterval time.Duration

	logger goengine.Logger
}

// NewListener returns a new notification listener
func NewListener(
	dbDSN string,
	dbChannel string,
	minReconnectInterval time.Duration,
	maxReconnectInterval time.Duration,
	logger goengine.Logger,
) (*Listener, error) {
	switch {
	case strings.TrimSpace(dbDSN) == "":
		return nil, goengine.InvalidArgumentError("dbDSN")
	case strings.TrimSpace(dbChannel) == "":
		return nil, goengine.InvalidArgumentError("dbChannel")
	case minReconnectInterval == 0:
		return nil, goengine.InvalidArgumentError("minReconnectInterval")
	case maxReconnectInterval < minReconnectInterval:
		return nil, goengine.InvalidArgumentError("maxReconnectInterval")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}

	return &Listener{
		dbDSN:                dbDSN,
		dbChannel:            dbChannel,
		minReconnectInterval: minReconnectInterval,
		maxReconnectInterval: maxReconnectInterval,
		logger:               logger,
	}, nil
}

// Listen start listening on the configures dbChannel and when a notification is received call the trigger
func (s *Listener) Listen(ctx context.Context, exec sql.ProjectionTrigger) error {
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
			s.logger.WithError(err).Warn("failed to close database Listener")
		}
	}()

	// Start listening to postgres notifications
	if err := listener.Listen(s.dbChannel); err != nil {
		return err
	}

	// Execute an initial run of the projection.
	// This is done after db listen is started to avoid losing a set of messages while the Listener creates a db connection.
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
func (s *Listener) listenerStateCallback(event pq.ListenerEventType, err error) {
	logger := s.logger.WithField("listener_event", event)
	if err != nil {
		logger = logger.WithError(err)
	}

	switch event {
	case pq.ListenerEventConnected:
		logger.Debug("connection Listener: connected")
	case pq.ListenerEventConnectionAttemptFailed:
		logger.Debug("connection Listener: failed to connect")
	case pq.ListenerEventDisconnected:
		logger.Debug("connection Listener: disconnected")
	case pq.ListenerEventReconnected:
		logger.Debug("connection Listener: reconnected")
	default:
		logger.Warn("connection Listener: unknown event")
	}
}

// unmarshalNotification takes a postgres notification and unmarshal it into a eventStoreNotification
func (s *Listener) unmarshalNotification(n *pq.Notification) *sql.ProjectionNotification {
	if n == nil {
		s.logger.Info("received nil notification")
		return nil
	}

	logger := s.logger.WithField("pq_notification", n)
	if n.Extra == "" {
		logger.Error("received notification without extra data")
		return nil
	}

	notification := &sql.ProjectionNotification{}
	if err := json.Unmarshal([]byte(n.Extra), notification); err != nil {
		logger.WithError(err).Error("received invalid notification data")
		return nil
	}

	logger.WithField("notification", notification).Debug("received notification")

	return notification
}
