package cete

// Close closes the database (all file handlers to the database).
func (d *DB) Close() {
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
