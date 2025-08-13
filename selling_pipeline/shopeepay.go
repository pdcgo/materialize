package selling_pipeline

import (
	"encoding/json"
	"log"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

type DailyShopeepayPipeline struct {
	ctx     *yenstream.RunnerContext
	badgedb *badger.DB
	metric  metric.Metric[*metric.DailyShopeepayBalance]
	exact   exact_one.ExactlyOnce
}

func NewDailyShopeepayPipeline(
	ctx *yenstream.RunnerContext,
	badgedb *badger.DB,
	metric metric.Metric[*metric.DailyShopeepayBalance],
	exact exact_one.ExactlyOnce,
) *DailyShopeepayPipeline {
	return &DailyShopeepayPipeline{
		ctx:     ctx,
		badgedb: badgedb,
		metric:  metric,
		exact:   exact,
	}
}

func (ds *DailyShopeepayPipeline) All(source yenstream.Pipeline) yenstream.Pipeline {
	cost := ds.Cost(source)
	topup := ds.Topup(source)
	refund := ds.Refund(source)
	difamount := ds.DiffAmount(source)
	return yenstream.NewFlatten(ds.ctx, "flatten_daily_shopeepay",
		cost,
		topup,
		refund,
		difamount,
	)
}

func (ds *DailyShopeepayPipeline) DiffAmount(source yenstream.Pipeline) yenstream.Pipeline {
	difCalc := NewDiffAccountCalc(ds.badgedb, ds.exact)
	diffamount := source.
		Via("filter_diff_amount", yenstream.NewFlatMap(ds.ctx, func(cdata *stat_replica.CdcMessage) ([]*metric.DailyShopeepayBalance, error) {
			var err error
			items := []*metric.DailyShopeepayBalance{}
			if cdata.SourceMetadata.Table != "balance_account_histories" {
				return items, nil
			}

			diffs, err := difCalc.ProcessCDC(cdata)
			if err != nil {
				raw, _ := json.Marshal(cdata)
				log.Println(string(raw))
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
		Via("diff_amount_merge_metric", yenstream.NewMap(ds.ctx, func(item *metric.DailyShopeepayBalance) (*metric.DailyShopeepayBalance, error) {

			err := ds.metric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
				if acc == nil {
					return item
				}

				acc.ActualDiffAmount += item.ActualDiffAmount
				return acc
			})
			return item, err
		}))

	return diffamount
}

func (ds *DailyShopeepayPipeline) Refund(source yenstream.Pipeline) yenstream.Pipeline {
	refund := source.
		Via("just_refund", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "inv_resolutions" {
				return false, nil
			}

			datamap := cdata.Data.(*models.InvResolution)
			rtipe := datamap.RefundPaymentType

			return rtipe == db_models.RestockPaymentShopeePay, nil
		})).
		Via("exact_one_refund", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			var err error
			var exist bool

			data := cdata.Data.(*models.InvResolution)
			saver := ds.exact.Change(data)
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
		Via("refund_save_metric", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			data := cdata.Data.(*models.InvResolution)
			day := data.FundAt.Local().Format("2006-01-02")

			item := &metric.DailyShopeepayBalance{
				Day:          day,
				TeamID:       data.TeamID,
				RefundAmount: data.RefundAmount,
			}
			err := ds.metric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
				if acc == nil {
					return item
				}

				acc.RefundAmount += item.RefundAmount

				return acc
			})

			return cdata, err
		}))

	return refund
}

func (ds *DailyShopeepayPipeline) Topup(source yenstream.Pipeline) yenstream.Pipeline {

	topup := source.
		Via("just_expense", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "expense_histories" {
				return false, nil
			}

			var err error
			var exist bool

			data := cdata.Data.(*models.ExpenseHistory)
			if data.CategoryID != 5 {
				return false, nil
			}

			saver := ds.exact.Change(data).Exist(&exist)

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
		Via("save_topup", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
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
			err = ds.metric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
				if acc == nil {
					return item
				}

				acc.TopupAmount += item.TopupAmount
				return acc
			})

			return cdata, err
		}))

	return topup
}

func (ds *DailyShopeepayPipeline) Cost(source yenstream.Pipeline) yenstream.Pipeline {
	cost := source.
		Via("just_cost_restock", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			var err error

			if cdata.SourceMetadata.Table != "restock_costs" {
				return false, nil
			}

			data := cdata.Data.(*models.RestockCost)
			if data.PaymentType != db_models.RestockPaymentShopeePay {
				return false, nil
			}
			// edit belum work

			exist := cdata.OldData != nil
			return !exist, err
		})).
		Via("save_metric_cost", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			data := cdata.Data.(*models.RestockCost)

			inv := &models.InvTransaction{
				ID: data.InvTransactionID,
			}

			_, err = ds.exact.GetItemStruct(inv)
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
			err = ds.metric.Merge(item.Key(), func(acc *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
				if acc == nil {
					return item
				}

				acc.CostAmount += item.CostAmount
				return acc
			})

			return cdata, nil
		}))

	return cost
}
