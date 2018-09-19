package internal

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/lib/pq"
)

type dataLayer struct {
	t            *testing.T
	db           *sql.DB
	databaseName string
}

// TestDatabase has callback to conduct integration tests with real database
func TestDatabase(t *testing.T, testCase func(db *sql.DB)) {
	// Fetch the postgres dsn from the env var
	osDSN, exists := os.LookupEnv("POSTGRES_DSN")
	if !exists {
		t.Fatal("extensions: postgres dsn does not exist")
	}

	// Parse the postgres dsn
	parsedDSN, err := pq.ParseURL(osDSN)
	if err != nil {
		t.Fatalf("extensions: failed to parse postgres uri (%v)", err)
	}

	r := regexp.MustCompile("dbname=(((\\\\ )|[^ ])+)")
	matches := r.FindStringSubmatchIndex(parsedDSN)

	// Extract the postgres db name and replace it with postgres
	dbName := parsedDSN[matches[2]:matches[3]]
	postgresDSN := parsedDSN[:matches[0]] + "dbname=postgres" + parsedDSN[matches[1]:]

	// Open db connection
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("Connection failed: %+v", err)
	}
	defer db.Close()

	dl := &dataLayer{
		t:            t,
		db:           db,
		databaseName: dbName,
	}
	dl.createSchema()
	testCase(db)
	dl.dropSchema()

}

func (dl dataLayer) dropSchema() {
	dl.disableDatabaseAccess()
	defer dl.enableDatabaseAccess()
	_, err := dl.db.Exec("DROP SCHEMA IF EXISTS public CASCADE")
	if err != nil {
		dl.t.Fatalf("Fail to drop schema. %s", err.Error())
	}
}

func (dl dataLayer) createSchema() {
	_, err := dl.db.Exec("CREATE SCHEMA IF NOT EXISTS public")
	if err != nil {
		dl.t.Fatalf("Fail to create schema. %s", err.Error())
	}
}

func (dl *dataLayer) disableDatabaseAccess() {
	// Making sure the database exists
	row := dl.db.QueryRow("SELECT datname FROM pg_database WHERE datname = $1", dl.databaseName)
	if row == nil {
		// No database so no one has access
		return
	}
	// Disallow new connections
	_, err := dl.db.Exec(fmt.Sprintf("ALTER DATABASE \"%s\" WITH ALLOW_CONNECTIONS false", dl.databaseName))
	if err != nil {
		dl.t.Fatalf("Unable to disallow connections to the db (%v)", err)
	}

	// Terminate existing connections
	row = dl.db.QueryRow("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", dl.databaseName)
}

func (dl *dataLayer) enableDatabaseAccess() {
	_, err := dl.db.Exec(fmt.Sprintf("ALTER DATABASE \"%s\" WITH ALLOW_CONNECTIONS true", dl.databaseName))
	if err != nil {
		dl.t.Fatalf("Unable to allow connections to the db (%v)", err)
	}
}
