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

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/backfill"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/gathering"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
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

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("❌ Interrupt received. Shutting down...")
		cancel()
	}()

	err = registerCoder(ctx)
	if err != nil {
		panic(err)
	}

	cdataChan := make(chan *stat_replica.CdcMessage, 9)
	repcfg := &stat_replica.ReplicationConfig{
		SlotName:        "stat_slot",
		PublicationName: "stat_publication",
		SlotTemporary:   true,
	}
	source := NewCDCStream(ctx,
		&backfill.BackfillConfig{
			StartTime: time.Now().AddDate(0, -1, 0),
		},
		repcfg,
		cdataChan,
	)

	go func() {
		defer close(cdataChan)
		err = source.
			Init().
			Backfill().
			Stream().
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

	exact := exact_one.NewBadgeExactOne(ctx, badgedb)

	// inisiasi metric
	// gather := metric.NewDefaultGather()
	pgGather := gathering.NewPostgresGather(ctx)

	// shopeeBalanceMetric := selling_metric.NewDailyShopeepayBalanceMetric(
	// 	badgedb,
	// 	exact,
	// )

	// running untuk sync ke postgres
	go func() {
	PP:
		for {
			select {
			case <-ctx.Done():
				break PP
			default:
				if source.GetStatus() == ReplicaMode {
					break PP
				}
			}
		}

		err := pgGather.StartSync()
		if err != nil {
			panic(err)
		}
	}()

	err = yenstream.
		NewRunnerContext(ctx).
		CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
			source := createLogStream(ctx, yenstream.NewChannelSource(ctx, cdataChan))
			sourcePipe := selling_pipeline.ExactOne(ctx, exact, source)

			shopDailyMetric, shopDailyStream := selling_metric.NewShopDailyMetric(ctx, exact, badgedb, sourcePipe)
			pgGather.AddMetric("daily_shop", shopDailyMetric)

			shopeeBalanceMetric := selling_metric.
				NewMetric(
					"shopee_balance",
					ctx,
					badgedb,
					exact,
					time.Second*5,
					func() *metric.DailyShopeepayBalance {
						return &metric.DailyShopeepayBalance{}
					},
					func(data *metric.DailyShopeepayBalance) (*metric.DailyShopeepayBalance, error) {
						team := &models.Team{
							ID: data.TeamID,
						}

						exact.GetItemStruct(team)
						data.TeamName = team.Name

						data.DiffAmount = data.RefundAmount + data.TopupAmount - data.CostAmount
						data.ErrDiffAmount = data.ActualDiffAmount - data.DiffAmount

						return data, err
					},
				)

			pgshopeeBalance := shopeeBalanceMetric.
				Via("save_postgres", yenstream.NewMap(ctx, func(met *metric.DailyShopeepayBalance) (*metric.DailyShopeepayBalance, error) {
					err := pgGather.SaveItem(met)
					return met, err
				}))

			dailyBalanceShopeepay := selling_pipeline.NewDailyShopeepayPipeline(
				ctx,
				badgedb,
				shopeeBalanceMetric,
				exact,
			)

			spayBalance := dailyBalanceShopeepay.All(sourcePipe)

			return yenstream.NewFlatten(ctx, "flatten",
				pgshopeeBalance,
				spayBalance.
					Via("silent", silent(ctx)),
				shopDailyStream.
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
