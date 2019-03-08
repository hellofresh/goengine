package config

import (
	"os"
	"strings"

	"github.com/hellofresh/goengine"
)

const (
	// EventStoreStreamName is the name of the event stream used for you bank account
	EventStoreStreamName goengine.StreamName = "bank"
	// StreamProjectionsTable is the name of the table used to store the state of the stream projections
	StreamProjectionsTable = "bank_projection"
	// AggregateProjectionAccountReportTable is the name of the table used to store the states of the account average aggregate projection
	AggregateProjectionAccountReportTable = "account_averages_projection"
)

// PostgresDSN the Postgres DSN used to connect to progres
var PostgresDSN string

func init() {
	PostgresDSN = os.Getenv("POSTGRES_DSN")
	if strings.TrimSpace(PostgresDSN) == "" {
		panic("expected POSTGRES_DSN to be set and not empty")
	}
}
