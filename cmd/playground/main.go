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

	"github.com/pdcgo/materialize/backfill_pipeline"
	"github.com/pdcgo/materialize/backfill_pipeline/backfill"
	"github.com/pdcgo/materialize/coders"
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/gathering"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/stat_db"
	"github.com/pdcgo/materialize/stat_replica"
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
	defer badgedb.DropAll()

	conn, err := backfill.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}

	exact := exact_one.NewBadgeExactOne(ctx, badgedb)

	// inisiasi metric
	// gather := metric.NewDefaultGather()
	pgGather := gathering.NewPostgresGather(ctx)

	shopeeBalanceMetric := selling_metric.NewDailyShopeepayBalanceMetric(
		badgedb,
		exact,
	)
	shopDailyMetric := selling_metric.NewDailyShopMetric(badgedb, exact)
	teamDailyMetric := selling_metric.NewDailyTeamMetric(badgedb, exact)
	bankBalanceMetric := selling_metric.NewDailyBankBalance(badgedb)

	// running untuk sync ke postgres
	// go func() {
	// PP:
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			break PP
	// 		default:
	// 			if source.GetStatus() == ReplicaMode {
	// 				break PP
	// 			}
	// 		}
	// 	}

	// 	err := pgGather.StartSync()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()

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

			dailyBalanceShopeepay := selling_pipeline.NewDailyShopeepayPipeline(
				ctx,
				badgedb,
				shopeeBalanceMetric,
				exact,
			)

			shopDailyStream := selling_metric.NewMetricStream(
				ctx,
				time.Second*5,
				shopDailyMetric,
				selling_pipeline.NewShopDailyPipeline(
					ctx,
					badgedb,
					shopDailyMetric,
					exact,
				).
					All(sourcePipe),
			)

			shopDailySink := shopDailyStream.
				DataChanges(badgedb).
				Via("saving", yenstream.NewMap(ctx,
					func(met *selling_metric.DailyShopMetricData) (*selling_metric.DailyShopMetricData, error) {
						err := pgGather.SaveItem(met)
						return met, err
					}))

			teamDailyStream := selling_metric.NewMetricStream(
				ctx,
				time.Second*5,
				teamDailyMetric,
				selling_pipeline.NewDailyTeamPipeline(
					ctx,
					teamDailyMetric,
				).
					All(shopDailyStream.CounterChanges()),
			)

			teamDailySink := teamDailyStream.
				DataChanges(badgedb).
				// Via("filter", yenstream.NewFilter(ctx, func(met *selling_metric.DailyTeamMetricData) (bool, error) {
				// 	if met.TeamID == 31 {
				// 		return true, nil
				// 	}

				// 	return false, nil
				// })).
				// Via("log", debug_pipeline.NewLogFile(ctx, "test.stream")).
				Via("saving", yenstream.NewMap(ctx,
					func(met *selling_metric.DailyTeamMetricData) (*selling_metric.DailyTeamMetricData, error) {
						err := pgGather.SaveItem(met)
						return met, err
					}))

			spayBalance := selling_metric.
				NewMetricStream(ctx, time.Second*5, shopeeBalanceMetric, dailyBalanceShopeepay.All(sourcePipe)).
				DataChanges(badgedb).
				Via("saving", yenstream.NewMap(ctx,
					func(met *metric.DailyShopeepayBalance) (*metric.DailyShopeepayBalance, error) {
						err := pgGather.SaveItem(met)
						return met, err
					}))

			bankBalance := selling_pipeline.
				NewDailyBankPipeline(ctx, bankBalanceMetric).
				All(teamDailyStream.CounterChanges())

			bankBalanceSink := selling_metric.NewMetricStream(
				ctx,
				time.Second*5,
				bankBalanceMetric,
				bankBalance,
			).
				DataChanges(badgedb).
				Via("save_bank_balance", yenstream.NewMap(ctx,
					func(met *selling_metric.DailyBankBalance) (*selling_metric.DailyBankBalance, error) {
						err := pgGather.SaveItem(met)
						return met, err
					}))

			return yenstream.NewFlatten(ctx, "flatten",
				bankBalanceSink.
					Via("silent", silent(ctx)),
				teamDailySink.
					Via("silent", silent(ctx)),
				spayBalance.
					Via("silent", silent(ctx)),
				shopDailySink.
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

func StructToMap(obj interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &result)
	return result, err
}
