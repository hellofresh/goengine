package config

import (
	"os"
	"strings"

	"github.com/hellofresh/goengine"
)

const (
	EventStoreStreamName goengine.StreamName = "bank"

	StreamProjectionsTable = "bank_projection"

	AggregateProjectionAccountReportTable = "account_averages_projection"
)

var PostgresDSN string

func init() {
	PostgresDSN = os.Getenv("POSTGRES_DSN")
	if strings.TrimSpace(PostgresDSN) == "" {
		panic("expected POSTGRES_DSN to be set and not empty")
	}
}
