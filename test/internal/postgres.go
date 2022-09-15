//go:build integration
// +build integration

package internal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var postgresControl *dbController

type dbController struct {
	db *sql.DB
}

func postgresController(t *testing.T) *dbController {
	if postgresControl == nil {
		dsn := postgresDSN(t)

		// Extract the postgres db name and replace it with postgres
		matches := postgresDSNDatabaseMatch(dsn)
		postgresDSN := dsn[:matches[0]] + "dbname=postgres" + dsn[matches[1]:]

		// Open the connection
		postgresDB, err := sql.Open("postgres", postgresDSN)
		require.NoError(t, err, "test.postgres: failed to connect to postgres db")

		postgresControl = &dbController{postgresDB}
	}

	return postgresControl
}

func (c *dbController) Drop(t *testing.T, databaseName string) {
	c.disableDatabaseAccess(t, databaseName)

	_, err := c.db.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, databaseName))
	require.NoError(t, err, "test.postgres: Fail to drop database")
}

func (c *dbController) Create(t *testing.T, databaseName string) {
	_, err := c.db.Exec(fmt.Sprintf(`CREATE DATABASE "%s"`, databaseName))
	if err != nil {
		// If the database already exists continue
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42P04" {
			c.enableDatabaseAccess(t, databaseName)
			return
		}

		require.NoError(t, err, "test.postgres: Fail to create database")
	}

	c.enableDatabaseAccess(t, databaseName)
}

func (c *dbController) disableDatabaseAccess(t *testing.T, databaseName string) {
	// Making sure the database exists
	row := c.db.QueryRow("SELECT datname FROM pg_database WHERE datname = $1", databaseName)
	if err := row.Scan(&databaseName); err != nil {
		// No database so no one has access
		if err != sql.ErrNoRows {
			return
		}
		require.NoError(t, err)
		return
	}

	// Disallow new connections
	_, err := c.db.Exec(fmt.Sprintf(`ALTER DATABASE "%s" WITH ALLOW_CONNECTIONS false`, databaseName))
	require.NoError(t, err, "test.postgres: Unable to disallow connections to the db")

	// Terminate existing connections
	rows, err := c.db.Query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", databaseName)
	require.NoError(t, err)
	require.NoError(t, err, rows.Close())
}

func (c *dbController) enableDatabaseAccess(t *testing.T, databaseName string) {
	_, err := c.db.Exec(fmt.Sprintf(`ALTER DATABASE "%s" WITH ALLOW_CONNECTIONS true`, databaseName))
	require.NoError(t, err, "test.postgres: Unable to allow connections to the db")
}

const postgresAppName = "goengine_dev_tests"

var (
	_ suite.SetupTestSuite    = &PostgresSuite{}
	_ suite.TearDownTestSuite = &PostgresSuite{}
)

// PostgresSuite testify suite that will create and drop a database before and after a test
type PostgresSuite struct {
	Suite

	PostgresDSN string

	controller *dbController
	db         *sql.DB
	dbName     string
	schemaName string
}

// SetupTest creates a database before a test
func (s *PostgresSuite) SetupTest() {
	s.Suite.SetupTest()

	s.controller = postgresController(s.T())

	s.PostgresDSN = postgresDSN(s.T())
	dsnMatches := postgresDSNDatabaseMatch(s.PostgresDSN)
	s.dbName = s.PostgresDSN[dsnMatches[2]:dsnMatches[3]]

	schemaMatch := postgresDSNSchemaMatch(s.PostgresDSN)
	// custom schema is set - remember it to create later
	if len(schemaMatch) > 0 {
		// trim schema name quotes from the matched positions - that's why we get +/-1 here
		s.schemaName = s.PostgresDSN[schemaMatch[2]+1 : schemaMatch[3]-1]
	}

	// Create the schema to use
	s.controller.Create(s.T(), s.dbName)
}

// DB returns a database connection pool for managed by the suite
func (s *PostgresSuite) DB() *sql.DB {
	if s.db == nil {
		var err error
		s.db, err = sql.Open("postgres", s.PostgresDSN)
		s.Require().NoError(err, "test.postgres: Connection failed")
		s.Require().NoError(s.db.Ping(), "test.postgres: Failed to Ping db")

		if s.schemaName != "" {
			_, err = s.db.Exec("CREATE SCHEMA IF NOT EXISTS " + s.schemaName)
			s.Require().NoError(err, "test.postgres: Failed to create db schema")
		}
	}

	return s.db
}

// DBTableExists return true if the provided table name exists in the public table schema of the suite's database
func (s *PostgresSuite) DBTableExists(tableName string) bool {
	var currentSchema string
	err := s.db.QueryRow(`select current_schema()`).Scan(&currentSchema)
	s.Require().NoError(err, "failed to get current schema")

	var exists bool
	err = s.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		currentSchema, tableName,
	).Scan(&exists)

	s.Require().NoError(err, "failed to check if table %s exists", tableName)

	return exists
}

// DBQueryIsRunningWithTimeout Check if a query matching the regex is currently running
func (s *PostgresSuite) DBQueryIsRunningWithTimeout(queryRegex *regexp.Regexp, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		res, err := s.DB().QueryContext(
			ctx,
			`SELECT query FROM pg_stat_activity WHERE datname = $1 AND application_name=$2`,
			s.dbName,
			postgresAppName,
		)
		if err != nil {
			return false, err
		}

		for res.Next() {
			var query string
			if err := res.Scan(&query); err != nil {
				return false, err
			}

			if queryRegex.MatchString(query) {
				return true, nil
			}
		}

		select {
		case <-ctx.Done():
			return false, nil
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TearDownTest drops the database create by SetupTest
func (s *PostgresSuite) TearDownTest() {
	if err := s.db.Close(); err != nil {
		s.T().Errorf("test.postgres: Connection failed to close: %+v", err)
	}
	s.db = nil

	s.controller.Drop(s.T(), s.dbName)

	s.Suite.TearDownTest()
}

// postgresDSN returns a parsed postgres dsn
func postgresDSN(t *testing.T) string {
	// Fetch the postgres dsn from the env var
	osDSN, exists := os.LookupEnv("POSTGRES_DSN")
	require.True(t, exists, "test.postgres: missing POSTGRES_DSN environment variable")

	// Parse the postgres dsn
	parsedDSN, err := pq.ParseURL(osDSN)
	require.NoError(t, err, "test.postgres: failed to parse postgres dsn")

	// Set the connection application name
	if strings.Contains(parsedDSN, "application_name") {
		parsedDSN = regexp.MustCompile("application_name=([^ ]|$)*").
			ReplaceAllString(parsedDSN, "application_name="+postgresAppName)
	} else {
		parsedDSN += " application_name=" + postgresAppName
	}

	parsedDSN = regexp.MustCompile(`dbname='((?:(\\ )|[^ ])+)'`).
		ReplaceAllString(parsedDSN, fmt.Sprintf("dbname=${1}-%d", time.Now().UnixNano()))

	return parsedDSN
}

// postgresDSNDatabaseMatch locate the dbname within the dsn and return the indexes
func postgresDSNDatabaseMatch(dsn string) []int {
	r := regexp.MustCompile(`dbname=(((\\ )|[^ ])+)`)
	matches := r.FindStringSubmatchIndex(dsn)

	return matches
}

// postgresDSNSchemaMatch locate the search_path within the dsn and return the indexes
func postgresDSNSchemaMatch(dsn string) []int {
	r := regexp.MustCompile(`search_path=(((\\ )|[^ ])+)`)
	matches := r.FindStringSubmatchIndex(dsn)

	return matches
}
