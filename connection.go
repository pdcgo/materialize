package materialize

import (
	"fmt"

	"gorm.io/gorm"
)

type Materialize struct {
	matdb *gorm.DB
}

type MatPgConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// CreateConnectionSQL generates the SQL statement to create a Postgres connection in Materialize.
// It includes IF NOT EXISTS to avoid errors if the connection already exists.
func (mat *Materialize) CreateSource(name string, cfg *MatPgConfig) error {
	sql := fmt.Sprintf(
		`CREATE CONNECTION IF NOT EXISTS %s
TO POSTGRES (
	HOST '%s',
	PORT %d,
	USER '%s',
	PASSWORD SECRET %s,
	DATABASE '%s',
	SSL MODE '%s'
);`,
		name,
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.Database,
		cfg.SSLMode,
	)
	return mat.matdb.Exec(sql).Error
}

// ListConnections retrieves the names of all connections in Materialize.
func (mat *Materialize) ListConnections() ([]string, error) {
	var connections []string
	rows, err := mat.matdb.Raw("SHOW CONNECTIONS").Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		// The first column is the connection name
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		connections = append(connections, name)
	}
	return connections, nil
}

// DeleteConnection drops a connection from Materialize by name.
func (mat *Materialize) DeleteConnection(name string) error {
	sql := fmt.Sprintf("DROP CONNECTION IF EXISTS %s;", name)
	return mat.matdb.Exec(sql).Error
}

func NewMaterialize(db *gorm.DB) *Materialize {
	return &Materialize{
		matdb: db,
	}
}
