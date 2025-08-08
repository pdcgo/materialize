package stat_replica

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/pkg/secret"
)

func ConnectProdDatabase(ctx context.Context) (*pgconn.PgConn, error) {
	var cfg configs.AppConfig
	var sec *secret.Secret
	var err error
	// getting database

	// getting configuration
	sec, err = secret.GetSecret("app_config_prod", "latest")
	if err != nil {
		return nil, err
	}
	err = sec.YamlDecode(&cfg)
	if err != nil {
		return nil, err
	}
	cfg.Database.DBInstance = "/cloudsql/" + cfg.Database.DBInstance
	dsn := cfg.Database.ToDsn("stat_streaming")
	dsn += " replication=database"

	// dsn := "host=/cloudsql/project:region:instance user=postgres password=yourpass dbname=yourdb sslmode=disable"
	dbconf, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	dbconf.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		matches := strings.Split(addr, `\`)
		if len(matches) <= 2 {
			return nil, fmt.Errorf("failed to parse addr: %q. It should conform to the regular expression", addr)
		}
		instance := matches[2]
		return proxy.Dial(instance)
	}

	conn, err := pgconn.ConnectConfig(ctx, dbconf)

	if err != nil {
		return conn, err
	}

	return conn, nil
}

// type dialer struct{}

// func (d dialer) Dial(ntw, addr string) (net.Conn, error) {
// 	matches := instanceRegexp.FindStringSubmatch(addr)
// 	if len(matches) != 2 {
// 		return nil, fmt.Errorf("failed to parse addr: %q. It should conform to the regular expression %q", addr, instanceRegexp)
// 	}
// 	instance := matches[1]
// 	return proxy.Dial(instance)
// }
// func (d dialer) DialTimeout(ntw, addr string, timeout time.Duration) (net.Conn, error) {
// 	return nil, fmt.Errorf("timeout is not currently supported for cloudsqlpostgres dialer")
// }

func connectWithConnector() (*sql.DB, error) {
	mustGetenv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			log.Fatalf("Fatal Error in connect_connector.go: %s environment variable not set.\n", k)
		}
		return v
	}
	// Note: Saving credentials in environment variables is convenient, but not
	// secure - consider a more secure solution such as
	// Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
	// keep passwords and other secrets safe.
	var (
		dbUser                 = mustGetenv("DB_USER")                  // e.g. 'my-db-user'
		dbPwd                  = mustGetenv("DB_PASS")                  // e.g. 'my-db-password'
		dbName                 = mustGetenv("DB_NAME")                  // e.g. 'my-database'
		instanceConnectionName = mustGetenv("INSTANCE_CONNECTION_NAME") // e.g. 'project:region:instance'
		usePrivate             = os.Getenv("PRIVATE_IP")
	)

	dsn := fmt.Sprintf("user=%s password=%s database=%s", dbUser, dbPwd, dbName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	var opts []cloudsqlconn.Option
	if usePrivate != "" {
		opts = append(opts, cloudsqlconn.WithDefaultDialOptions(cloudsqlconn.WithPrivateIP()))
	}
	// WithLazyRefresh() Option is used to perform refresh
	// when needed, rather than on a scheduled interval.
	// This is recommended for serverless environments to
	// avoid background refreshes from throttling CPU.
	opts = append(opts, cloudsqlconn.WithLazyRefresh())
	d, err := cloudsqlconn.NewDialer(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	// Use the Cloud SQL connector to handle connecting to the instance.
	// This approach does *NOT* require the Cloud SQL proxy.
	config.DialFunc = func(ctx context.Context, network, instance string) (net.Conn, error) {
		return d.Dial(ctx, instanceConnectionName)
	}
	dbURI := stdlib.RegisterConnConfig(config)
	dbPool, err := sql.Open("pgx", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return dbPool, nil
}
