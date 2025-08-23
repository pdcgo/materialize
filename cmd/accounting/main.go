package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pdcgo/materialize/accounting_core"
	"github.com/pdcgo/materialize/accounting_transaction/stock_transaction"
	"github.com/pdcgo/materialize/backfill_pipeline"
	"github.com/pdcgo/materialize/backfill_pipeline/backfill"
	"github.com/pdcgo/materialize/coders"
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/gathering"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_process/stat_db"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

func initializeDirectory() {
	os.Mkdir("./streamdata/", os.ModeDir)
}

func main() {
	var err error
	var cancel context.CancelFunc

	initializeDirectory()

	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)
	ctx = stat_replica.ContextWithCoder(ctx)
	ctx = selling_metric.ContextWithMetricControl(ctx)

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("❌ Interrupt received. Shutting down...")
		cancel()
	}()
	err = coders.WarehouseCoder(ctx)
	if err != nil {
		panic(err)
	}

	cdataChan := make(chan *stat_replica.CdcMessage, 9)
	repcfg := &stat_replica.ReplicationConfig{
		SlotName:        "stat_slot",
		PublicationName: "stat_publication",
		SlotTemporary:   true,
	}

	backfilCfg := &backfill.BackfillConfig{
		StartTime: time.Now().AddDate(0, 0, -5),
	}

	source := NewCDCStream(ctx,
		backfilCfg,
		repcfg,
		cdataChan,
	)

	go func() {
		defer close(cdataChan)
		err = source.
			Init().
			Backfill().
			// Stream().
			Err()

		if err != nil {
			slog.Error(err.Error(), slog.String("process", "streaming data"))
		}

	}()

	badgedb, err := stat_db.NewBadgeDB("./streamdata/replication")
	if err != nil {
		panic(err)
	}
	defer badgedb.Close()
	// defer badgedb.DropAll()

	conn, err := backfill.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}

	// local docker database
	gormdb, err := gathering.CreateDB()
	if err != nil {
		panic(err)
	}
	err = accounting_core.GormAutoMigrate(gormdb)
	if err != nil {
		panic(err)
	}

	exact := exact_one.NewBadgeExactOne(ctx, badgedb)

	err = yenstream.
		NewRunnerContext(ctx).
		CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
			// source := createLogStream(ctx, yenstream.NewChannelSource(ctx, cdataChan))
			source := yenstream.NewChannelSource(ctx, cdataChan).Via("dummy", yenstream.NewMap(ctx, func(d any) (any, error) {
				return d, nil
			}))

			sideload := backfill_pipeline.NewSideload(ctx, conn, exact)

			sloadsource := sideload.
				All(source)

			sourcePipe := selling_pipeline.
				ExactOne(ctx, exact, sloadsource)

			stockOps := stock_transaction.NewStockTransaction(gormdb)

			stock := sourcePipe.
				Via("stock", yenstream.NewFilter(ctx,
					func(cdata *stat_replica.CdcMessage) (bool, error) {
						if cdata.SourceMetadata.Table != "inv_transactions" {
							return false, nil
						}

						if cdata.OldData != nil {
							return false, nil
						}

						data := cdata.Data.(*models.InvTransaction)
						switch data.Type {
						case db_models.InvTxRestock:
							return true, nil
						}

						return false, nil
					})).
				Via("check_account", yenstream.NewMap(ctx,
					func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
						data := cdata.Data.(*models.InvTransaction)
						teamID := data.TeamID

						keys := []accounting_core.AccountKey{
							accounting_core.CashAccount,
							accounting_core.StockPendingAccount,
						}

						for _, key := range keys {

							acc := accounting_core.Account{
								AccountKey:  key,
								TeamID:      teamID,
								Coa:         accounting_core.ASSET,
								BalanceType: accounting_core.DebitBalance,
								Name:        fmt.Sprintf("Team %d (%s)", teamID, key),
								Created:     time.Now(),
							}

							found, err := exact.GetItemStruct(&acc)
							if err != nil {
								return cdata, err
							}

							if !found {

								err = gormdb.Save(&acc).Error
								// if err != nil {
								// 	return cdata, err
								// }
								err = exact.Change(&acc).Save().Err()
								if err != nil {
									return cdata, err
								}
							}
						}

						return cdata, nil
					})).
				Via("stock ops", yenstream.NewMap(ctx,
					func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
						data := cdata.Data.(*models.InvTransaction)

						err := stockOps.Restock(&stock_transaction.RestockPayload{
							TeamID:        data.TeamID,
							WarehouseID:   data.WarehouseID,
							RestockAmount: data.Total,
						})

						if err != nil {
							return cdata, err
						}

						return cdata, nil
					}))

			return yenstream.NewFlatten(ctx, "flatten",
				stock.
					Via("silent", silent(ctx)),
			).
				Via("log", yenstream.NewMap(ctx, func(data any) (any, error) {
					raw, err := json.Marshal(data)
					slog.Info(string(raw))

					return data, err
				}))

		}).
		Err()

	if err != nil {
		slog.Error("processor stopped", slog.String("err", err.Error()))
	}

	// datas := shopeeBalanceMetric.ToSlice()
	// sort.Slice(datas, func(i, j int) bool {
	// 	return datas[i].Day > datas[j].Day
	// })
	// // var _ CsvItem = (*metric.DailyShopeepayBalance)(nil)
	// err = SaveCsv("./streamdata/shopeepay_30day.csv", datas)
	// if err != nil {
	// 	panic(err)
	// }

	// for _, data := range datas {
	// 	raw, _ := json.Marshal(data)
	// 	log.Println(string(raw))
	// }
}

func silent(ctx *yenstream.RunnerContext) yenstream.Pipeline {
	return yenstream.NewFilter(ctx, func(data any) (bool, error) {
		return false, nil
	})
}
