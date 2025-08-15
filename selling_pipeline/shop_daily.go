package selling_pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

type DailyShopPipeline struct {
	ctx     *yenstream.RunnerContext
	badgedb *badger.DB
	metric  metric.MetricStore[*selling_metric.DailyShopMetricData]
	exact   exact_one.ExactlyOnce
}

func (ds *DailyShopPipeline) Return(source yenstream.Pipeline) yenstream.Pipeline {
	returnSource := source.
		Via("dshop_return_filter", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "inv_transactions" {
				return false, nil
			}

			data := cdata.Data.(*models.InvTransaction)
			if data.Type != db_models.InvTxReturn {
				return false, nil
			}

			switch cdata.ModType {
			case stat_replica.CdcBackfill:
				if cdata.OldData == nil {
					return true, nil
				}
			case stat_replica.CdcInsert, stat_replica.CdcUpdate:
				return true, nil
			}

			return false, nil
		}))

	arrived := returnSource.
		Via("dshop_only_backfill_update", yenstream.NewFlatMap(ds.ctx, func(cdata *stat_replica.CdcMessage) ([]*selling_metric.DailyShopMetricData, error) {
			data := cdata.Data.(*models.InvTransaction)
			result := []*selling_metric.DailyShopMetricData{}

			orddata := &models.InvOrderData{
				InvID: data.ID,
			}

			found, err := ds.exact.GetItemStruct(orddata)
			if err != nil {
				return result, err
			}

			if !found {
				return result, fmt.Errorf("order data not found inv id %d", data.ID)
			}

			switch cdata.ModType {
			case stat_replica.CdcBackfill:
				if data.Arrived.IsZero() {
					return result, nil
				}

				result = append(result, &selling_metric.DailyShopMetricData{
					Day:                 data.Arrived.Local().Format("2006-01-02"),
					ShopID:              orddata.MpID,
					TeamID:              data.TeamID,
					ReturnArrivedAmount: orddata.MpTotal,
				})
			case stat_replica.CdcUpdate:
				var oldarr time.Time

				old, ok := cdata.OldData.(*models.InvTransaction)
				if ok {
					oldarr = old.Arrived
				}

				if data.Arrived.Local().Compare(oldarr.Local()) != 0 {
					result = append(result, &selling_metric.DailyShopMetricData{
						Day:                 data.Arrived.Local().Format("2006-01-02"),
						ShopID:              orddata.MpID,
						TeamID:              data.TeamID,
						ReturnArrivedAmount: orddata.MpTotal,
					})
				}

			}
			return result, nil
		}))

	created := returnSource.
		Via("dshop_filter_update", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.ModType == stat_replica.CdcUpdate {
				return false, nil
			}
			return true, nil
		})).
		Via("dshop_return_add_metric", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			data := cdata.Data.(*models.InvTransaction)

			orddata := &models.InvOrderData{
				InvID: data.ID,
			}

			found, err := ds.exact.GetItemStruct(orddata)
			if err != nil {
				return cdata, err
			}

			if !found {
				return cdata, errors.New("inv order data not found")
			}

			item := &selling_metric.DailyShopMetricData{
				Day:                 data.Created.Local().Format("2006-01-02"),
				ShopID:              orddata.MpID,
				TeamID:              orddata.TeamID,
				ReturnCreatedAmount: orddata.MpTotal,
			}

			err = ds.metric.Merge(item.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
				if acc == nil {
					return item
				}

				acc.ReturnCreatedAmount += item.ReturnCreatedAmount
				return acc
			})

			return cdata, err
		}))

	return yenstream.NewFlatten(ds.ctx, "ds_return_flatten", created, arrived)
}

func (ds *DailyShopPipeline) Lost(source yenstream.Pipeline) yenstream.Pipeline {
	return source.
		Via("extract_lost", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.OldData != nil {
				return false, nil
			}
			if cdata.SourceMetadata.Table != "order_timestatmps" {
				return false, nil
			}

			return true, nil
		})).
		Via("lost_add_metric", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			data := cdata.Data.(*models.OrderTimestamp)

			ord := &models.Order{
				ID: data.OrderID,
			}

			found, err := ds.exact.GetItemStruct(ord)
			if err != nil {
				return cdata, err
			}

			if !found {
				return cdata, errors.New("order not found")
			}

			item := &selling_metric.DailyShopMetricData{
				Day:             data.Timestamp.Local().Format("2006-01-02"),
				ShopID:          ord.OrderMpID,
				TeamID:          ord.TeamID,
				LostOrderAmount: float64(ord.OrderMpTotal),
			}
			switch data.OrderStatus {
			case db_models.OrdProblem:
				ds.metric.Merge(item.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return item
					}

					acc.LostOrderAmount += item.LostOrderAmount
					return acc
				})
			}
			return cdata, nil
		}))
}

func (ds *DailyShopPipeline) Problem(source yenstream.Pipeline) yenstream.Pipeline {
	return source.
		Via("extract_problem", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.OldData != nil {
				return false, nil
			}
			if cdata.SourceMetadata.Table != "order_timestatmps" {
				return false, nil
			}

			return true, nil
		})).
		Via("problem_add_metric", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			data := cdata.Data.(*models.OrderTimestamp)

			ord := &models.Order{
				ID: data.OrderID,
			}

			found, err := ds.exact.GetItemStruct(ord)
			if err != nil {
				return cdata, err
			}

			if !found {
				return cdata, errors.New("order not found")
			}

			item := &selling_metric.DailyShopMetricData{
				Day:                data.Timestamp.Local().Format("2006-01-02"),
				ShopID:             ord.OrderMpID,
				TeamID:             ord.TeamID,
				ProblemOrderAmount: float64(ord.OrderMpTotal),
			}
			switch data.OrderStatus {
			case db_models.OrdProblem:
				ds.metric.Merge(item.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return item
					}

					acc.ProblemOrderAmount += item.ProblemOrderAmount
					return acc
				})
			}
			return cdata, nil
		}))
}

func (ds *DailyShopPipeline) Cancel(source yenstream.Pipeline) yenstream.Pipeline {
	return source.
		Via("extract_cancel", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.OldData != nil {
				return false, nil
			}
			if cdata.SourceMetadata.Table != "order_timestatmps" {
				return false, nil
			}

			return true, nil
		})).
		Via("cancel_add_metric", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			data := cdata.Data.(*models.OrderTimestamp)

			ord := &models.Order{
				ID: data.OrderID,
			}

			found, err := ds.exact.GetItemStruct(ord)
			if err != nil {
				return cdata, err
			}

			if !found {
				return cdata, errors.New("order not found")
			}

			item := &selling_metric.DailyShopMetricData{
				Day:               data.Timestamp.Local().Format("2006-01-02"),
				ShopID:            ord.OrderMpID,
				TeamID:            ord.TeamID,
				CancelOrderAmount: float64(ord.OrderMpTotal),
			}
			switch data.OrderStatus {
			case db_models.OrdCancel:
				ds.metric.Merge(item.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return item
					}

					acc.CancelOrderAmount += item.CancelOrderAmount
					return acc
				})
			}
			return cdata, nil
		}))
}

func (ds *DailyShopPipeline) Ads(cdstream yenstream.Pipeline) yenstream.Pipeline {
	ads := cdstream.
		Via("process_ads", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "ads_expense_histories" {
				return false, nil
			}

			data := cdata.Data.(*models.AdsExpenseHistory)
			olddata := &models.AdsExpenseHistory{}
			var found bool
			err := ds.exact.
				Change(data).
				Before(&found, olddata).
				Save().
				Err()

			cdata.OldData = olddata
			if olddata.ID == 0 {
				cdata.OldData = nil
			}

			return true, err
		})).
		Via("add_metric_shop", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			var haveold, ok, samedate bool
			var old, data *models.AdsExpenseHistory

			old, haveold = cdata.OldData.(*models.AdsExpenseHistory)
			data, ok = cdata.Data.(*models.AdsExpenseHistory)

			if haveold {
				samedate = old.At.Compare(data.At) == 0
			} else {
				samedate = true
			}

			if !ok {
				return cdata, errors.New("data contain nil")
			}

			if !samedate && haveold {
				newitem := &selling_metric.DailyShopMetricData{
					ShopID:         data.MarketplaceID,
					TeamID:         data.TeamID,
					Day:            data.At.Local().Format("2006-01-02"),
					AdsSpentAmount: data.Amount,
				}

				err = ds.metric.Merge(newitem.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return newitem
					}
					acc.AdsSpentAmount += newitem.AdsSpentAmount
					return acc
				})

				if err != nil {
					return cdata, err
				}

				olditem := &selling_metric.DailyShopMetricData{
					ShopID:         old.MarketplaceID,
					TeamID:         old.TeamID,
					Day:            old.At.Local().Format("2006-01-02"),
					AdsSpentAmount: old.Amount,
				}

				err = ds.metric.Merge(olditem.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return newitem
					}
					acc.AdsSpentAmount -= olditem.AdsSpentAmount
					return acc
				})

				if err != nil {
					return cdata, err
				}

				return cdata, nil
			} else {

				newitem := &selling_metric.DailyShopMetricData{
					ShopID:         data.MarketplaceID,
					TeamID:         data.TeamID,
					Day:            data.At.Local().Format("2006-01-02"),
					AdsSpentAmount: 0,
				}

				err = ds.metric.Merge(newitem.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						newitem.AdsSpentAmount = data.Amount
						return newitem
					}

					switch cdata.ModType {
					case stat_replica.CdcBackfill:
						if haveold {
							return acc
						} else {
							acc.AdsSpentAmount += data.Amount
						}
					case stat_replica.CdcInsert:
						acc.AdsSpentAmount = data.Amount
					case stat_replica.CdcDelete:
						acc.AdsSpentAmount -= data.Amount
					case stat_replica.CdcUpdate:
						var amount float64
						if haveold {
							amount = data.Amount - old.Amount
						}
						acc.AdsSpentAmount += amount
					}

					return acc
				})

				if err != nil {
					return cdata, err
				}
			}

			return cdata, nil
		}))

	return ads
}

func (ds *DailyShopPipeline) All(cdstream yenstream.Pipeline) yenstream.Pipeline {
	cancel := ds.Cancel(cdstream)
	lost := ds.Lost(cdstream)
	problem := ds.Problem(cdstream)
	retur := ds.Return(cdstream)
	order := ds.Order(cdstream)
	ads := ds.Ads(cdstream)
	wd := ds.Withdrawal(cdstream)

	all := yenstream.NewFlatten(ds.ctx, "daily_shop_flatten",
		wd,
		ads,
		order,
		cancel,
		lost,
		problem,
		retur,
	)
	return all
}

func NewShopDailyPipeline(
	ctx *yenstream.RunnerContext,
	badgedb *badger.DB,
	metric metric.MetricStore[*selling_metric.DailyShopMetricData],
	exact exact_one.ExactlyOnce,
) *DailyShopPipeline {
	return &DailyShopPipeline{
		ctx:     ctx,
		badgedb: badgedb,
		metric:  metric,
		exact:   exact,
	}
}
