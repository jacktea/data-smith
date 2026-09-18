package ident

import "strings"

// Style identifies the delimiter used by a SQL dialect for identifiers.
type Style byte

const (
	DoubleQuote Style = '"'
	Backtick    Style = '`'
)

// Quote returns an identifier delimited for the requested dialect. Embedded
// delimiter characters are escaped by doubling them, as required by both
// PostgreSQL and MySQL.
func Quote(style Style, identifier string) string {
	delimiter := string(style)
	return delimiter + strings.ReplaceAll(identifier, delimiter, delimiter+delimiter) + delimiter
}

// Qualified returns a quoted schema-qualified object name. When schema is
// empty, only the object name is returned.
func Qualified(style Style, schema, name string) string {
	if schema == "" {
		return Quote(style, name)
	}
	return Quote(style, schema) + "." + Quote(style, name)
}

// List quotes identifiers and joins them with separator.
func List(style Style, identifiers []string, separator string) string {
	quoted := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		quoted[i] = Quote(style, identifier)
	}
	return strings.Join(quoted, separator)
}
