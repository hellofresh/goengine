package pq

import (
	"context"
	"strings"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/lib/pq"
	"github.com/mailru/easyjson"
)

// Ensure Listener implements sql.Listener
var _ sql.Listener = &Listener{}

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
			s.logger.Warn("failed to close database Listener", func(e goengine.LoggerEntry) {
				e.Error(err)
			})
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
			s.logger.Debug("context closed stopping projection", nil)
			return nil
		}
	}
}

// listenerStateCallback a callback used for getting state changes from a pq.Listener
// This callback will also close the related db connection pool used to query/persist projection data
func (s *Listener) listenerStateCallback(event pq.ListenerEventType, err error) {
	logFields := func(e goengine.LoggerEntry) {
		e.Int("listener_event", int(event))
		if err != nil {
			e.Error(err)
		}
	}

	switch event {
	case pq.ListenerEventConnected:
		s.logger.Debug("connection Listener: connected", logFields)
	case pq.ListenerEventConnectionAttemptFailed:
		s.logger.Debug("connection Listener: failed to connect", logFields)
	case pq.ListenerEventDisconnected:
		s.logger.Debug("connection Listener: disconnected", logFields)
	case pq.ListenerEventReconnected:
		s.logger.Debug("connection Listener: reconnected", logFields)
	default:
		s.logger.Warn("connection Listener: unknown event", logFields)
	}
}

// unmarshalNotification takes a postgres notification and unmarshal it into a eventStoreNotification
func (s *Listener) unmarshalNotification(n *pq.Notification) *sql.ProjectionNotification {
	if n == nil {
		s.logger.Info("received nil notification", nil)
		return nil
	}

	if n.Extra == "" {
		s.logger.Error("received notification without extra data", func(e goengine.LoggerEntry) {
			e.Any("pq_notification", n)
		})
		return nil
	}

	notification := &sql.ProjectionNotification{}
	if err := easyjson.Unmarshal([]byte(n.Extra), notification); err != nil {
		s.logger.Error("received invalid notification data", func(e goengine.LoggerEntry) {
			e.Any("pq_notification", n)
			e.Error(err)
		})
		return nil
	}

	s.logger.Debug("received notification", func(e goengine.LoggerEntry) {
		e.Any("pq_notification", n)
		e.Int64("notification.no", notification.No)
		e.String("notification.aggregate_id", notification.AggregateID)
	})

	return notification
}
