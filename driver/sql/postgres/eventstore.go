package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrNoCreateTableQueries occurs when table create queries are not presented in the strategy
	ErrNoCreateTableQueries = errors.New("goengine: create table queries are not provided")
	// ErrTableAlreadyExists occurs when table cannot be created as it exists already
	ErrTableAlreadyExists = errors.New("goengine: table already exists")
	// ErrTableNameEmpty occurs when table cannot be created because it has an empty name
	ErrTableNameEmpty = errors.New("goengine: table name could not be empty")

	// Ensure that we satisfy the eventstore.EventStore interface
	_ goengine.EventStore = &EventStore{}
	// Ensure that we satisfy the ReadOnlyEventStore interface
	_ driverSQL.ReadOnlyEventStore = &EventStore{}
)

// EventStore a in postgres event store implementation
type EventStore struct {
	persistenceStrategy driverSQL.PersistenceStrategy
	db                  *sql.DB
	messageFactory      driverSQL.MessageFactory
	columns             string
	columnCount         int
	eventColumns        string
	logger              goengine.Logger
}

// NewEventStore return a new postgres.EventStore
func NewEventStore(
	persistenceStrategy driverSQL.PersistenceStrategy,
	db *sql.DB,
	messageFactory driverSQL.MessageFactory,
	logger goengine.Logger,
) (*EventStore, error) {
	switch {
	case persistenceStrategy == nil:
		return nil, goengine.InvalidArgumentError("persistenceStrategy")
	case db == nil:
		return nil, goengine.InvalidArgumentError("db")
	case messageFactory == nil:
		return nil, goengine.InvalidArgumentError("messageFactory")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}

	columns := persistenceStrategy.ColumnNames()
	insertColumns := make([]string, len(columns))
	for i, c := range columns {
		insertColumns[i] = QuoteIdentifier(c)
	}

	columns = persistenceStrategy.EventColumnNames()
	selectColumns := make([]string, len(columns))
	for i, c := range columns {
		selectColumns[i] = QuoteIdentifier(c)
	}

	return &EventStore{
		persistenceStrategy: persistenceStrategy,
		db:                  db,
		messageFactory:      messageFactory,
		columns:             strings.Join(insertColumns, ", "),
		columnCount:         len(insertColumns),
		eventColumns:        strings.Join(selectColumns, ", "),
		logger:              logger,
	}, nil
}

// Create creates the database table, index etc needed for the event stream
func (e *EventStore) Create(ctx context.Context, streamName goengine.StreamName) error {
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

	if len(queries) == 1 {
		_, err := e.db.ExecContext(ctx, queries[0])
		return err
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, q := range queries {
		_, err := tx.ExecContext(ctx, q)
		if err == nil {
			continue
		}

		if errRollback := tx.Rollback(); errRollback != nil {
			e.logger.Error("could not rollback transaction", func(e goengine.LoggerEntry) {
				e.Error(errRollback)
				e.String("query", q)
			})
		}

		return err
	}

	return tx.Commit()
}

// HasStream returns true if the table for the eventstream already exists
func (e *EventStore) HasStream(ctx context.Context, streamName goengine.StreamName) bool {
	tableName, err := e.tableName(streamName)
	if err != nil {
		return false
	}

	return e.tableExists(ctx, tableName)
}

// Load returns an eventstream based on the provided constraints
func (e *EventStore) Load(
	ctx context.Context,
	streamName goengine.StreamName,
	fromNumber int64,
	count *uint,
	matcher metadata.Matcher,
) (goengine.EventStream, error) {
	return e.loadQuery(ctx, e.db, streamName, fromNumber, count, matcher)
}

// LoadWithConnection returns an eventstream based on the provided constraints using the provided sql.Conn
func (e *EventStore) LoadWithConnection(
	ctx context.Context,
	conn driverSQL.Queryer,
	streamName goengine.StreamName,
	fromNumber int64,
	count *uint,
	matcher metadata.Matcher,
) (goengine.EventStream, error) {
	return e.loadQuery(ctx, conn, streamName, fromNumber, count, matcher)
}

// loadQuery returns an eventstream based on the provided constraints
// This func is used by Load and LoadWithConnection.
func (e *EventStore) loadQuery(
	ctx context.Context,
	db driverSQL.Queryer,
	streamName goengine.StreamName,
	fromNumber int64,
	count *uint,
	matcher metadata.Matcher,
) (goengine.EventStream, error) {
	tableName, err := e.tableName(streamName)
	if err != nil {
		return nil, err
	}

	selectQuery := make([]byte, 0, 196)
	params := make([]interface{}, 0, 4)

	selectQuery = append(selectQuery, "SELECT "...)
	selectQuery = append(selectQuery, e.eventColumns...)
	selectQuery = append(selectQuery, " FROM "...)
	selectQuery = append(selectQuery, tableName...)

	// Add conditions to the select query
	selectQuery = append(selectQuery, " WHERE no >= $1"...)
	params = append(params, fromNumber)

	if matcher != nil {
		searchPart, searchParams := e.persistenceStrategy.PrepareSearch(matcher)
		selectQuery = append(selectQuery, searchPart...)
		params = append(params, searchParams...)
	}
	selectQuery = append(selectQuery, " ORDER BY no "...)
	if count != nil {
		selectQuery = append(selectQuery, "LIMIT "...)
		selectQuery = append(selectQuery, strconv.FormatUint(uint64(*count), 10)...)
	}

	rows, err := db.QueryContext(ctx, string(selectQuery), params...)
	if err != nil {
		return nil, err
	}

	return e.messageFactory.CreateEventStream(rows)
}

// AppendTo batch inserts Messages into the event stream table
func (e *EventStore) AppendTo(ctx context.Context, streamName goengine.StreamName, streamEvents []goengine.Message) error {
	return e.AppendToWithExecer(ctx, e.db, streamName, streamEvents)
}

// AppendToWithExecer batch inserts Messages into the event stream table using the provided Connection/Execer
func (e *EventStore) AppendToWithExecer(ctx context.Context, conn driverSQL.Execer, streamName goengine.StreamName, streamEvents []goengine.Message) error {
	eventCount := len(streamEvents)
	if eventCount == 0 {
		return nil
	}

	tableName, err := e.tableName(streamName)
	if err != nil {
		return err
	}

	data, err := e.persistenceStrategy.PrepareData(streamEvents)
	if err != nil {
		return err
	}

	insertQuery := make([]byte, 0, 35+len(e.columns)+(e.columnCount*2)+(eventCount*3))
	insertQuery = append(insertQuery, "INSERT INTO "...)
	insertQuery = append(insertQuery, tableName...)
	insertQuery = append(insertQuery, " ("...)
	insertQuery = append(insertQuery, e.columns...)
	insertQuery = append(insertQuery, ") VALUES "...)
	for i := 0; i < eventCount; i++ {
		if i != 0 {
			insertQuery = append(insertQuery, ',')
		}
		insertQuery = append(insertQuery, '(')
		for j := 0; j < e.columnCount; {
			if j != 0 {
				insertQuery = append(insertQuery, ',')
			}
			j++

			insertQuery = append(insertQuery, '$')
			insertQuery = append(insertQuery, strconv.Itoa((i*e.columnCount)+j)...)
		}
		insertQuery = append(insertQuery, ')')
	}

	result, err := conn.ExecContext(ctx, string(insertQuery), data...)
	if err != nil {
		e.logger.Warn("failed to insert messages into the event stream", func(e goengine.LoggerEntry) {
			e.Error(err)
			e.String("streamName", string(streamName))
			e.Any("streamEvents", streamEvents)
		})

		return err
	}

	e.logger.Debug("inserted messages into the event stream", func(e goengine.LoggerEntry) {
		e.Error(err)
		e.String("streamName", string(streamName))
		e.Any("streamEvents", streamEvents)
		e.Any("result", result)
	})

	return nil
}

func (e *EventStore) tableName(s goengine.StreamName) (string, error) {
	tableName, err := e.persistenceStrategy.GenerateTableName(s)
	if err != nil {
		return "", err
	}
	if len(tableName) == 0 {
		return "", ErrTableNameEmpty
	}
	return tableName, nil
}

func (e *EventStore) tableExists(ctx context.Context, tableName string) bool {
	var exists bool
	err := e.db.QueryRowContext(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`,
		tableName,
	).Scan(&exists)

	if err != nil {
		e.logger.Warn("error on reading from information_schema", func(e goengine.LoggerEntry) {
			e.Error(err)
			e.String("table", tableName)
		})

		return false
	}

	return exists
}
