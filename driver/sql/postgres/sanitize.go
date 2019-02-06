package postgres

import "strings"

// QuoteString returns the given string quoted
func QuoteString(str string) string {
	return "'" + strings.Replace(str, "'", "''", -1) + "'"
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.
func QuoteIdentifier(name string) string {
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
