package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pdcgo/materialize/stat_process/backfill"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_process/stat_db"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

var backfill_tables backfillTables = backfillTables{
	"teams",
	"warehouses",
	"account_types",
	"expense_categories",
	"expense_accounts",
}

func main() {

	ctx := stat_replica.ContextWithCoder(context.Background())
	stat_replica.RegisterCoderSource(ctx, &stat_replica.SourceMetadata{Table: "teams", Schema: "public"}, &models.Team{})

	ctx, cancel := context.WithCancel(ctx)
	badgedb, err := stat_db.NewBadgeDB("/tmp/replication")
	if err != nil {
		panic(err)
	}
	defer badgedb.Close()

	metricGather := metric.NewDefaultGather()

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cdcChan := make(chan *stat_replica.CdcMessage, 10)
	defer close(cdcChan)

	go func() {
		<-sigChan
		metricGather.Debug()
		fmt.Println("❌ Interrupt received. Shutting down...")
		cancel()

	}()

	go func() {

		runBackfill(ctx, cdcChan)
		runReplica(ctx, cdcChan)
	}()

	exact := exact_one.NewBadgeExactOne(ctx, badgedb)
	err = yenstream.
		NewRunnerContext(ctx).
		CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
			side := stat_db.NewSideloadDB(badgedb)

			source := yenstream.NewChannelSource(ctx, cdcChan)
			exactone := source.
				Via("filter exact one", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					return exact.AddItemWithKey("id", cdata)
				}))

			wareInvoiceMetric := metric.NewDefaultMetricStore(badgedb, func() *metric.DailyWarehouseInvoice {
				return &metric.DailyWarehouseInvoice{}
			}, nil)

			// shopeeBalanceMetric := metric.NewDefaultMetricStore(badgedb, func() *metric.DailyShopeepayBalance {
			// 	return &metric.DailyShopeepayBalance{}
			// })

			metricGather.AddMetric("warehouse_invoice", wareInvoiceMetric)

			invoice := exactone.
				Via("only_invoice_warehouse", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					if cdata.SourceMetadata.Table != "invoices" {
						return false, nil
					}

					dataMap := cdata.Data.(map[string]interface{})

					_, err := side.GetWithSchema(&stat_replica.SourceMetadata{
						Table:  "warehouses",
						Schema: "public",
					}, dataMap["to_team_id"])

					if err != nil {
						if stat_db.IsItemNotFound(err) {
							return false, nil
						}
						return false, err
					}

					if dataMap["status"] == db_models.InvoiceNotFinal {
						return false, nil
					}

					return cdata.SourceMetadata.Table == "invoices", nil
				})).
				Via("add_warehouse_metric", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
					dataMap := cdata.Data.(map[string]interface{})

					var amountN pgtype.Numeric = dataMap["amount"].(pgtype.Numeric)
					amount, err := amountN.Float64Value()

					var day string
					createdD := dataMap["created"]
					switch created := createdD.(type) {
					case time.Time:
						day = created.Local().Format("2006-01-02")
					case string:
						dd := strings.Split(created, "T")
						day = dd[0]
					default:
						tname := reflect.TypeOf(createdD).Name()
						panic(fmt.Sprintf("created not supported type %s", tname))
					}

					met := metric.DailyWarehouseInvoice{
						WarehouseID:   uint(dataMap["to_team_id"].(int64)),
						CreatedAmount: amount.Float64,
						Day:           day,
					}

					err = wareInvoiceMetric.Merge(met.Key(), func(acc *metric.DailyWarehouseInvoice) *metric.DailyWarehouseInvoice {
						if acc == nil {
							return &met
						}
						acc.CreatedAmount += met.CreatedAmount
						return acc
					})

					return cdata, err
				})).
				Via("log", debugstream(ctx))

			base := exactone
			return yenstream.NewFlatten(ctx, "flatten", invoice, base)
		}).Err()

	if err != nil {
		panic(err)
	}

}

type backfillTables []string

func (b backfillTables) Contains(tablename string) bool {
	for _, d := range b {
		if d == tablename {
			return true
		}
	}

	return false
}

func runBackfill(ctx context.Context, cdcChan chan<- *stat_replica.CdcMessage) {
	conn, err := backfill.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}

	defer conn.Close(ctx)

	tables := backfill.NewBackfillTable(ctx, conn, backfill_tables)
	tables.Start(func(cdata *stat_replica.CdcMessage) {
		cdcChan <- cdata
	})

	ord := backfill.NewBackfillOrder(ctx, conn)
	ord.Start(func(cdata *stat_replica.CdcMessage) {
		cdcChan <- cdata
	})

	inv := backfill.NewBackfillInvoice(ctx, conn)
	inv.Start(func(cdata *stat_replica.CdcMessage) {
		cdcChan <- cdata
	})

	balanceHist := backfill.NewBackfillBalanceHistories(ctx, conn, nil)
	balanceHist.Start(func(cdata *stat_replica.CdcMessage) {
		cdcChan <- cdata
	})

	log.Println("backfill completed")
}

func runReplica(ctx context.Context, cdcChan chan<- *stat_replica.CdcMessage) {
	var err error

	conn, err := stat_replica.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}

	defer conn.Close(ctx)

	// Initialize Replication
	initrep := stat_replica.NewInitReplica(ctx, conn)
	err = initrep.
		Initialize(true).
		Err()

	if err != nil {
		panic(err)
	}
	replication := stat_replica.NewReplication(ctx, conn)

	replication.AddHandler(func(msg *stat_replica.CdcMessage) {
		if msg == nil {
			return
		}

		switch msg.SourceMetadata.Table {
		case "orders",
			"invoices",
			"warehouses",
			"teams":
			cdcChan <- msg
		default:
			if backfill_tables.Contains(msg.SourceMetadata.Table) {
				cdcChan <- msg
			}
		}
	})

	err = replication.Start()
	if err != nil {
		panic(err)
	}
}

func debugstream(ctx *yenstream.RunnerContext) yenstream.Pipeline {
	return yenstream.NewMap(ctx, func(cdata any) (any, error) {
		raw, err := json.Marshal(cdata)
		log.Println(string(raw))
		return cdata, err
	})
}
