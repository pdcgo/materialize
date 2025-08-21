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
			if cdata.SourceMetadata.Table != "order_timestamps" {
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

func (ds *DailyShopPipeline) All(cdstream yenstream.Pipeline) yenstream.Pipeline {
	cancel := ds.Cancel(cdstream)
	lost := ds.Lost(cdstream)
	problem := ds.Problem(cdstream)
	retur := ds.Return(cdstream)
	order := ds.Order(cdstream)
	ads := ds.Ads(cdstream)
	wd := ds.Withdrawal(cdstream)
	ware := ds.WarehouseFee(cdstream)

	all := yenstream.NewFlatten(ds.ctx, "daily_shop_flatten",
		wd,
		ads,
		order,
		cancel,
		lost,
		problem,
		retur,
		ware,
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
