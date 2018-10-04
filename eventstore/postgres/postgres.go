package postgres

import "strings"

// quoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//    tblname := "my_table"
//    data := "my_data"
//    quoted := quoteIdentifier(tblname)
//    err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
func quoteIdentifier(name string) string {
	return quote(name, '"')
}

// quoteString quotes an string to be used as part of an SQL statement.
func quoteString(name string) string {
	return quote(name, '\'')
}

func quote(name string, quoteChar rune) string {
	q := string(quoteChar)
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return q + strings.Replace(name, q, q+q, -1) + q
}
