package cete

import "sync/atomic"

// Close closes the database (all file handlers to the database).
func (d *DB) Close() {
	atomic.StoreInt32(&d.closed, 1)

	for _, table := range d.tables {
		for _, index := range table.indexes {
			index.index.Close()
		}
		table.data.Close()
	}
}

// Tables returns the list of tables in the database.
func (d *DB) Tables() []string {
	var tables []string
	for name := range d.tables {
		tables = append(tables, string(name))
	}

	return tables
}

// Table returns the table with the given name. If the table does not exist,
// nil is returned.
func (d *DB) Table(tableName string) *Table {
	return d.tables[Name(tableName)]
}
