package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/pdcgo/materialize/stat_process/backfill"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/gathering"
	"github.com/pdcgo/materialize/stat_process/metric"
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
	gather := metric.NewDefaultGather()
	pgGather := gathering.NewPostgresGather(ctx)

	shopeeBalanceMetric := metric.NewDefaultMetricStore(
		badgedb,
		func() *metric.DailyShopeepayBalance {
			return &metric.DailyShopeepayBalance{}
		},
		func(data *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
			team := &models.Team{
				ID: data.TeamID,
			}

			exact.GetItemStruct(team)
			data.TeamName = team.Name

			data.DiffAmount = data.RefundAmount + data.TopupAmount - data.CostAmount
			data.ErrDiffAmount = data.ActualDiffAmount - data.DiffAmount
			return data
		},
	)
	gather.AddMetric("daily_shopeepay_balance", shopeeBalanceMetric)
	pgGather.AddMetric("daily_shopeepay_balance", shopeeBalanceMetric)

	// running untuk sync ke postgres
	err = pgGather.StartSync()
	if err != nil {
		panic(err)
	}

	// mulai stream
	difCalc := metric.NewDiffAccountCalc(badgedb, exact)
	err = yenstream.
		NewRunnerContext(ctx).
		CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
			source := yenstream.NewChannelSource(ctx, cdataChan)

			exactone := source.
				Via("exact_one", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					var err error
					var ok bool

					switch cdata.SourceMetadata.Table {
					case
						"inv_resolutions",
						"expense_histories",
						"inv_transactions",
						"restock_costs",
						"invoice_payment_submission",
						"product_tags",
						"product_category":
						return false, nil

					case "teams":
						data := cdata.Data.(*models.Team)
						err = exact.
							Change(data).
							Save().
							Err()

						return false, err

					default:
						ok, err = exact.AddItemWithKey("id", cdata)
						return ok, err
					}

				}))

			cost := source.
				Via("just_cost_restock", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					var err error
					switch cdata.SourceMetadata.Table {
					case "restock_costs":
					case "inv_transactions":
						data := cdata.Data.(*models.InvTransaction)
						// var exist bool
						err = exact.
							Change(data).
							// Exist(&exist).
							Save().
							Err()
						return false, err

					default:
						return false, nil
					}

					data := cdata.Data.(*models.RestockCost)
					if data.PaymentType != db_models.RestockPaymentShopeePay {
						return false, nil
					}

					if data.ShippingFee == 0 {
						return false, nil
					}
					var exist bool
					err = exact.
						Change(data).
						Exist(&exist).
						Save().
						Err()

					return !exist, err
				})).
				Via("save_metric_cost", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
					var err error
					data := cdata.Data.(*models.RestockCost)

					inv := &models.InvTransaction{
						ID: data.InvTransactionID,
					}

					_, err = exact.GetItemStruct(inv)
					if err != nil {
						return nil, err
					}
					// belum detect update
					cost := inv.Total + data.CodFee

					day := inv.Created.Local().Format("2006-01-02")
					item := &metric.DailyShopeepayBalance{
						Day:        day,
						TeamID:     inv.TeamID,
						CostAmount: cost,
					}
					err = shopeeBalanceMetric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
						if acc == nil {
							return item
						}

						acc.CostAmount += item.CostAmount
						return acc
					})

					return cdata, nil
				})).
				Via("silent", silent(ctx))

			topup := source.
				Via("just_expense", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					if cdata.SourceMetadata.Table != "expense_histories" {
						return false, nil
					}

					var err error
					var exist bool

					data := cdata.Data.(*models.ExpenseHistory)
					if data.CategoryID != 5 {
						return false, nil
					}

					saver := exact.Change(data).Exist(&exist)

					switch cdata.ModType {
					case stat_replica.CdcDelete:
						if exist {
							err = saver.Delete().Err()
						}
					default:
						err = saver.
							Save().
							Err()
					}

					return !exist, err
				})).
				Via("save_topup", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
					var err error
					data := cdata.Data.(*models.ExpenseHistory)
					day := data.At.Local().Format("2006-01-02")
					var amount float64
					switch cdata.ModType {
					case stat_replica.CdcDelete:
						amount = data.Amount * -1
					default:
						amount = data.Amount
					}

					item := &metric.DailyShopeepayBalance{
						Day:         day,
						TeamID:      data.TeamID,
						TopupAmount: amount,
					}
					err = shopeeBalanceMetric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
						if acc == nil {
							return item
						}

						acc.TopupAmount += item.TopupAmount
						return acc
					})

					return cdata, err
				})).
				Via("silent", silent(ctx))

			refund := source.
				Via("just_refund", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					if cdata.SourceMetadata.Table != "inv_resolutions" {
						return false, nil
					}

					datamap := cdata.Data.(*models.InvResolution)
					rtipe := datamap.RefundPaymentType

					return rtipe == db_models.RestockPaymentShopeePay, nil
				})).
				Via("exact_one_refund", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
					var err error
					var exist bool

					data := cdata.Data.(*models.InvResolution)
					saver := exact.Change(data)
					err = saver.
						Exist(&exist).
						Err()

					if err != nil {
						return false, err
					}

					if !exist {
						return true, saver.Save().Err()
					}

					return false, nil
				})).
				Via("refund_save_metric", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
					data := cdata.Data.(*models.InvResolution)
					day := data.FundAt.Local().Format("2006-01-02")

					item := &metric.DailyShopeepayBalance{
						Day:          day,
						TeamID:       data.TeamID,
						RefundAmount: data.RefundAmount,
					}
					err := shopeeBalanceMetric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
						if acc == nil {
							return item
						}

						acc.RefundAmount += item.RefundAmount

						return acc
					})

					return cdata, err
				})).
				Via("silent", silent(ctx))

			diffamount := exactone.
				// Via("silent", silent(ctx)).
				Via("filter_diff_amount", yenstream.NewFlatMap(ctx, func(cdata *stat_replica.CdcMessage) ([]*metric.DailyShopeepayBalance, error) {
					var err error
					items := []*metric.DailyShopeepayBalance{}
					if cdata.SourceMetadata.Table != "balance_account_histories" {
						return items, nil
					}

					diffs, err := difCalc.ProcessCDC(cdata)
					if err != nil {
						panic(err)
					}
					for _, diff := range diffs {
						// if diff.AccountID != 45 {
						// 	continue
						// }
						if diff.Camount != 0 {
							items = append(items, &metric.DailyShopeepayBalance{
								Day:              diff.Day,
								TeamID:           diff.TeamID,
								ActualDiffAmount: diff.Camount,
							})
						}

					}

					return items, err
				})).
				// Via("filter_debug", yenstream.NewFilter(ctx, func(item *metric.DailyShopeepayBalance) (bool, error) {
				// 	return item.TeamID == 31, nil
				// })).
				Via("diff_amount_merge_metric", yenstream.NewMap(ctx, func(item *metric.DailyShopeepayBalance) (*metric.DailyShopeepayBalance, error) {

					err := shopeeBalanceMetric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
						if acc == nil {
							return item
						}

						acc.ActualDiffAmount += item.ActualDiffAmount
						return acc
					})
					return item, err
				})).
				Via("silent", silent(ctx))

			return yenstream.NewFlatten(ctx, "flatten",
				refund,
				topup,
				diffamount,
				cost,
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

	datas := shopeeBalanceMetric.ToSlice()
	sort.Slice(datas, func(i, j int) bool {
		return datas[i].Day > datas[j].Day
	})
	// var _ CsvItem = (*metric.DailyShopeepayBalance)(nil)
	err = SaveCsv("./streamdata/shopeepay_30day.csv", datas)
	if err != nil {
		panic(err)
	}

	for _, data := range datas {
		raw, _ := json.Marshal(data)
		log.Println(string(raw))
	}
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
