package database

import (
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/postgres"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/pkg/secret"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ConnectToProduction(maxcon int, appName string) (*gorm.DB, error) {
	var cfg configs.AppConfig
	var sec *secret.Secret
	var err error

	// getting configuration
	sec, err = secret.GetSecret("app_config_prod", "latest")
	if err != nil {
		return nil, err
	}
	err = sec.YamlDecode(&cfg)
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		DriverName: "cloudsqlpostgres",
		DSN:        cfg.Database.ToDsn(appName),
	}), &gorm.Config{})
	if err != nil {
		return db, err
	}

	sqldb, err := db.DB()
	if err != nil {
		return db, err
	}

	sqldb.SetMaxOpenConns(maxcon)
	return db, err
}
