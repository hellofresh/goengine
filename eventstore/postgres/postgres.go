package postgres

import "strings"

// DoubleQuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//    tblname := "my_table"
//    data := "my_data"
//    quoted := pq.DoubleQuoteIdentifier(tblname)
//    err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
func DoubleQuoteIdentifier(name string) string {
	return quoteIdentifier(name, '"')
}

// SingleQuoteIdentifier for JSONB field
func SingleQuoteIdentifier(name string) string {
	return quoteIdentifier(name, '\'')
}

func quoteIdentifier(name string, quoteChar rune) string {
	q := string(quoteChar)
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return q + strings.Replace(name, q, q+q, -1) + q
}
