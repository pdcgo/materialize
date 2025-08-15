package backfill

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/pkg/secret"
)

func ConnectProdDatabase(ctx context.Context) (*pgx.Conn, error) {
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
	dsn := cfg.Database.ToDsn("stat_backfill")

	// dsn := "host=/cloudsql/project:region:instance user=postgres password=yourpass dbname=yourdb sslmode=disable"
	dbconf, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	// dbconf.DialFunc =

	dbconf.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		matches := strings.Split(addr, `\`)
		if len(matches) <= 2 {
			return nil, fmt.Errorf("failed to parse addr: %q. It should conform to the regular expression", addr)
		}
		instance := matches[2]
		return proxy.Dial(instance)
	}

	conn, err := pgx.ConnectConfig(ctx, dbconf)

	if err != nil {
		return conn, err
	}

	return conn, nil
}

// func RowStructParser(ctx context.Context, tablename string, rows pgx.Rows, handle func(cddata *stat_replica.CdcMessage)) error {
// 	r := pgxscan.NewRowScanner(rows)
// 	for rows.Next() {

// 		err := r.Scan(&team)
// 		if err != nil {
// 			panic(err)
// 		}

// 		cddata := stat_replica.CdcMessage{
// 			SourceMetadata: &stat_replica.SourceMetadata{
// 				Table:    tablename,
// 				Schema:   "public",
// 				Database: "",
// 			},
// 			ModType:   stat_replica.CdcBackfill,
// 			Data:      rowMap,
// 			Timestamp: time.Now().UnixMicro(),
// 		}

// 		handle(&cddata)
// 	}

// 	return nil
// }

func RowParser(ctx context.Context, tablename string, rows pgx.Rows, handle func(cddata *stat_replica.CdcMessage)) error {
	var err error
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	numCols := len(fieldDescriptions)

	meta := &stat_replica.SourceMetadata{
		Table:    tablename,
		Schema:   "public",
		Database: "",
	}

	for rows.Next() {
		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}

		rowMap := make(map[string]interface{}, numCols)
		for i, fd := range fieldDescriptions {
			colName := string(fd.Name)
			rowMap[colName] = values[i]
		}

		var coder interface{}
		coder, err = stat_replica.GetCoder(ctx, meta.PrefixKey())
		if err != nil {
			if !errors.Is(err, stat_replica.ErrCoderNotFound) {
				return err
			}
		}

		if coder != nil {
			err = stat_replica.MapToStruct(rowMap, coder)
			if err != nil {
				return err
			}
		} else {
			coder = rowMap
		}

		cddata := stat_replica.CdcMessage{
			SourceMetadata: meta,
			ModType:        stat_replica.CdcBackfill,
			Data:           coder,
			Timestamp:      time.Now().UnixMicro(),
		}

		handle(&cddata)
	}
	return rows.Err()
}
