package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/hellofresh/goengine/eventstore"
	esql "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrNoAggregateStreamStrategy error on no aggregate stream strategy provided
	ErrNoAggregateStreamStrategy = errors.New("no aggregate stream strategy provided")
	// ErrNoCreateTableQueries occurs when table create queries are not presented in the strategy
	ErrNoCreateTableQueries = errors.New("create table queries are not provided")
	// ErrNoDBConnect error on no DB connection provided
	ErrNoDBConnect = errors.New("no DB connection provided")
	// ErrNoMessageFactory error on no message factory provided
	ErrNoMessageFactory = errors.New("sql message factory should be provided")
	// ErrTableAlreadyExists occurs when table cannot be created as it exists already
	ErrTableAlreadyExists = errors.New("table name could not be empty")
	// ErrTableNameEmpty occurs when create is called for an already created stream
	ErrTableNameEmpty = errors.New("table name could not be empty")
	// Ensure that we satisfy the eventstore.EventStore interface
	_ eventstore.EventStore = &EventStore{}
)

// EventStore a in postgres event store implementation
type EventStore struct {
	persistenceStrategy       eventstore.PersistenceStrategy
	db                        *sql.DB
	messageFactory            esql.MessageFactory
	preparedInsertPlaceholder map[int]string
	columns                   string
	logger                    logrus.FieldLogger
}

// NewEventStore return a new postgres.EventStore
func NewEventStore(
	persistenceStrategy eventstore.PersistenceStrategy,
	db *sql.DB,
	messageFactory esql.MessageFactory,
	logger logrus.FieldLogger,
) (eventstore.EventStore, error) {
	if persistenceStrategy == nil {
		return nil, ErrNoAggregateStreamStrategy
	}
	if db == nil {
		return nil, ErrNoDBConnect
	}
	if messageFactory == nil {
		return nil, ErrNoMessageFactory
	}

	columns := fmt.Sprintf("%s", strings.Join(persistenceStrategy.ColumnNames(), ", "))

	return &EventStore{
		persistenceStrategy:       persistenceStrategy,
		db:                        db,
		messageFactory:            messageFactory,
		preparedInsertPlaceholder: make(map[int]string),
		columns:                   columns,
		logger:                    logger,
	}, nil
}

// Create creates the database table, index etc needed for the event stream
func (e *EventStore) Create(ctx context.Context, streamName eventstore.StreamName) error {
	tableName, err := e.tableName(streamName)
	if err != nil {
		return err
	}

	if e.tableExists(ctx, tableName) {
		return ErrTableAlreadyExists
	}

	queries := e.persistenceStrategy.CreateSchema(tableName)
	if len(queries) == 0 {
		return ErrNoCreateTableQueries
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, q := range queries {
		_, err := e.db.ExecContext(ctx, q)
		if err == nil {
			continue
		}
		errRollback := tx.Rollback()
		if errRollback != nil {
			return fmt.Errorf("error one: %s\nerror two: %s", errRollback, err)
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// HasStream returns true if the table for the eventstream already exists
func (e *EventStore) HasStream(ctx context.Context, streamName eventstore.StreamName) bool {
	tableName, err := e.tableName(streamName)
	if err != nil {
		return false
	}

	return e.tableExists(ctx, tableName)
}

// Load returns the eventstream based on the given constraints
func (e *EventStore) Load(
	ctx context.Context,
	streamName eventstore.StreamName,
	fromNumber int,
	count *uint,
	matcher metadata.Matcher,
) (eventstore.EventStream, error) {
	tableName, err := e.tableName(streamName)
	if err != nil {
		return nil, err
	}

	conditions, params := matchConditions(matcher)
	condition := fmt.Sprintf("no >= $%d", len(params)+1)
	conditions = append(conditions, condition)
	params = append(params, fromNumber)
	where := fmt.Sprintf("WHERE %s", strings.Join(conditions, " AND "))

	limit := ""
	if count != nil {
		limit = fmt.Sprintf("LIMIT %d", *count)
	}

	q := fmt.Sprintf(
		`SELECT * FROM %s %s ORDER BY no %s`,
		tableName,
		where,
		limit,
	)

	rows, err := e.db.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, err
	}

	return e.messageFactory.CreateEventStream(rows)
}

// AppendTo batch inserts Messages into the event stream table
func (e *EventStore) AppendTo(ctx context.Context, streamName eventstore.StreamName, streamEvents []messaging.Message) error {
	tableName, err := e.tableName(streamName)
	if err != nil {
		return err
	}
	columns := e.persistenceStrategy.ColumnNames()
	lenCols := len(columns)
	values := e.prepareInsertValues(streamEvents, lenCols)

	q := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s;",
		tableName,
		e.columns,
		values,
	)

	data, err := e.persistenceStrategy.PrepareData(streamEvents)
	if err != nil {
		return err
	}

	result, err := e.db.ExecContext(ctx, q, data...)
	if err != nil {
		return err
	}
	if e.logger != nil {
		e.logger.
			WithFields(logrus.Fields{
				"queryResult":  result,
				"streamName":   streamName,
				"streamEvents": streamEvents,
			}).
			WithError(err).
			Debugf("batch inserts Messages into the event stream table")
	}
	return err
}

func (e *EventStore) prepareInsertValues(streamEvents []messaging.Message, lenCols int) string {
	messageCount := len(streamEvents)
	if messageCount == 0 {
		return ""
	}
	if values, ok := e.preparedInsertPlaceholder[messageCount]; ok {
		return values
	}

	placeholders := bytes.NewBufferString("")

	placeholderCount := messageCount * lenCols
	for i := 0; i < placeholderCount; i++ {
		if m := i % lenCols; m == 0 {
			if i != 0 {
				placeholders.WriteString("),")
			}
			placeholders.WriteRune('(')
		} else {
			placeholders.WriteRune(',')
		}

		placeholders.WriteRune('$')
		placeholders.WriteString(strconv.Itoa(i + 1))
	}
	placeholders.WriteString(")")
	e.preparedInsertPlaceholder[messageCount] = placeholders.String()

	return e.preparedInsertPlaceholder[messageCount]
}

func (e *EventStore) tableName(s eventstore.StreamName) (string, error) {
	tableName, err := e.persistenceStrategy.GenerateTableName(s)
	if err != nil {
		return "", err
	}
	if len(tableName) == 0 {
		return "", ErrTableNameEmpty
	}
	return tableName, nil
}

func matchConditions(matcher metadata.Matcher) ([]string, []interface{}) {
	var params []interface{}
	var conditions []string
	i := 0
	matcher.Iterate(func(c metadata.Constraint) {
		i++
		field := SingleQuoteIdentifier(c.Field())
		condition := fmt.Sprintf("metadata ->> %s %s $%d", field, c.Operator(), i)
		conditions = append(conditions, condition)
		params = append(params, c.Value())
	})

	return conditions, params
}

func (e *EventStore) tableExists(ctx context.Context, tableName string) bool {
	var exists bool
	err := e.db.QueryRowContext(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`,
		tableName,
	).Scan(&exists)

	if err != nil {
		if e.logger != nil {
			e.logger.Warnf("error on readinf from information_schema %s", err)
		}

		return false
	}

	return exists
}
