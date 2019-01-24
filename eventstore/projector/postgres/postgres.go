package postgres

import "strings"

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
