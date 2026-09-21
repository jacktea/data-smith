// Package rowloc selects dialect-independent row location information.
// SQL quoting, value rendering, placeholders, and statement assembly belong
// to dialect packages and deliberately do not appear here.
package rowloc

import (
	"sort"

	"github.com/jacktea/data-smith/pkg/conn"
)

// Field is one column/value pair used to locate a row.
type Field struct {
	Column   string
	DataType string
	Value    any
}

// Location describes how one record can be found. A reliable location is
// backed by a primary key or an ordinary NOT NULL unique index. Otherwise the
// complete record is used for DELETE matching and must not drive UPDATE.
type Location struct {
	Fields   []Field
	reliable bool
}

// Columns returns the selected location columns in deterministic order.
func (l Location) Columns() []string {
	columns := make([]string, 0, len(l.Fields))
	for _, field := range l.Fields {
		columns = append(columns, field.Column)
	}
	return columns
}

// Reliable reports whether the location is backed by a physical row identity.
func (l Location) Reliable() bool { return l.reliable }

// IdentityColumns selects a physical row identity: primary key first, then
// the lexicographically first ordinary NOT NULL unique index.
func IdentityColumns(table *conn.Table) []string {
	if table == nil {
		return nil
	}
	if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 {
		return append([]string(nil), table.PrimaryKey.Columns...)
	}
	indexNames := make([]string, 0, len(table.Indexes))
	for name := range table.Indexes {
		indexNames = append(indexNames, name)
	}
	sort.Strings(indexNames)
	for _, name := range indexNames {
		index := table.Indexes[name]
		if index == nil || !index.Unique || len(index.Columns) == 0 || index.Where != nil || index.Expression != nil {
			continue
		}
		reliable := true
		for _, columnName := range index.Columns {
			column := table.Columns[columnName]
			if column == nil || column.Nullable {
				reliable = false
				break
			}
		}
		if reliable {
			return append([]string(nil), index.Columns...)
		}
	}
	return nil
}

// Build chooses location columns and attaches the record values and column
// types needed by a dialect to render its own predicate.
func Build(table *conn.Table, row conn.Record) Location {
	columns := IdentityColumns(table)
	reliable := len(columns) > 0
	if !reliable {
		columns = make([]string, 0, len(row))
		for column := range row {
			columns = append(columns, column)
		}
		sort.Strings(columns)
	}
	location := Location{Fields: make([]Field, 0, len(columns)), reliable: reliable}
	for _, columnName := range columns {
		var dataType string
		if table != nil {
			if column := table.Columns[columnName]; column != nil {
				dataType = column.DataType
			}
		}
		location.Fields = append(location.Fields, Field{Column: columnName, DataType: dataType, Value: row[columnName]})
	}
	return location
}
