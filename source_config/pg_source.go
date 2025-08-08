package source_config

import (
	"fmt"

	"gorm.io/gorm"
)

type Publication struct {
	Name string `json:"name"`
}

type PostgresSource interface {
	ListPublication() ([]*Publication, error)
	ListTablesOfPublication(pubName string) ([]string, error)
	CreatePublication(name string, tables ...string) error
	DeletePublication(name string) error
	SetTableReplicaIdentity(option ReplicaIdentityOption, tableNames []string, indexName ...string) error
}

type postgresSourceImpl struct {
	db *gorm.DB
}

// ListPublication implements PostgresSource.
func (p *postgresSourceImpl) ListPublication() ([]*Publication, error) {
	pubs := []*Publication{}
	err := p.db.Raw("SELECT pubname AS name FROM pg_publication").Scan(&pubs).Error
	return pubs, err
}

func (p *postgresSourceImpl) CreatePublication(name string, tables ...string) error {
	var sql string
	var args []interface{}

	if len(tables) == 0 {
		sql = "CREATE PUBLICATION ? FOR ALL TABLES"
		args = append(args, gorm.Expr(name))
	} else {
		sql = "CREATE PUBLICATION ? FOR TABLE "
		args = append(args, gorm.Expr(name))
		for i, table := range tables {
			if i > 0 {
				sql += ", "
			}
			sql += "?"
			args = append(args, gorm.Expr(table))
		}
	}
	return p.db.Exec(sql, args...).Error
}

func (p *postgresSourceImpl) DeletePublication(name string) error {
	return p.db.Exec("DROP PUBLICATION IF EXISTS ?", gorm.Expr(name)).Error
}

func (p *postgresSourceImpl) ListTablesOfPublication(pubName string) ([]string, error) {
	var tables []string
	var pubAllTables bool

	// Check if publication is FOR ALL TABLES
	err := p.db.Raw("SELECT puballtables FROM pg_publication WHERE pubname = ?", pubName).Scan(&pubAllTables).Error
	if err != nil {
		return nil, err
	}

	if pubAllTables {
		// List all user tables in the database
		query := `
			SELECT relname
			FROM pg_class
			WHERE relkind = 'r'
			AND relnamespace IN (
				SELECT oid FROM pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema')
			)
		`
		err = p.db.Raw(query).Scan(&tables).Error
	} else {
		// List only tables in the publication
		query := `
			SELECT pt.relname
			FROM pg_publication pub
			JOIN pg_publication_rel pr ON pub.oid = pr.prpubid
			JOIN pg_class pt ON pr.prrelid = pt.oid
			WHERE pub.pubname = ?
		`
		err = p.db.Raw(query, pubName).Scan(&tables).Error
	}
	return tables, err
}

type ReplicaIdentityOption string

const (
	ReplicaIdentityDefault ReplicaIdentityOption = "DEFAULT"
	ReplicaIdentityFull    ReplicaIdentityOption = "FULL"
	ReplicaIdentityNothing ReplicaIdentityOption = "NOTHING"
	ReplicaIdentityIndex   ReplicaIdentityOption = "USING INDEX"
)

// SetTableReplicaIdentity sets the replica identity for the given tables.
// If option is ReplicaIdentityIndex, indexName must be provided.
func (p *postgresSourceImpl) SetTableReplicaIdentity(option ReplicaIdentityOption, tableNames []string, indexName ...string) error {
	for _, tableName := range tableNames {
		var sql string
		switch option {
		case ReplicaIdentityDefault, ReplicaIdentityFull, ReplicaIdentityNothing:
			sql = "ALTER TABLE " + tableName + " REPLICA IDENTITY " + string(option)
		case ReplicaIdentityIndex:
			if len(indexName) == 0 {
				return fmt.Errorf("index name must be provided for REPLICA IDENTITY USING INDEX")
			}
			sql = "ALTER TABLE " + tableName + " REPLICA IDENTITY USING INDEX " + indexName[0]
		default:
			return fmt.Errorf("unsupported replica identity option: %s", option)
		}
		if err := p.db.Exec(sql).Error; err != nil {
			return err
		}
	}
	return nil
}

func NewPostgresSource(db *gorm.DB) PostgresSource {
	return &postgresSourceImpl{
		db: db,
	}
}
