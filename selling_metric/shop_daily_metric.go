package selling_metric

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

type DailyShopMetricData struct {
	Day               string  `json:"day" gorm:"primaryKey"`
	ShopID            uint    `json:"shop_id" gorm:"primaryKey"`
	TeamID            uint    `json:"team_id"`
	AdsSpentAmount    float64 `json:"ads_spent_amount"`
	CancelOrderAmount float64 `json:"cancel_order_amount"`

	ReturnCreatedAmount float64 `json:"return_created_amount"`
	ReturnArrivedAmount float64 `json:"return_arrived_amount"`

	ProblemOrderAmount float64 `json:"problem_order_amount"`
	LostOrderAmount    float64 `json:"lost_order_amount"`

	CreatedOrderAmount    float64 `json:"created_order_amount"`
	SysCreatedOrderAmount float64 `json:"sys_created_order_amount"`

	EstWithdrawalAmount float64 `json:"est_withdrawal_amount"`
	WithdrawalAmount    float64 `json:"withdrawal_amount"`
	MpAdjustmentAmount  float64 `json:"mp_adjustment_amount"`
	AdjOrderAmount      float64 `json:"adj_order_amount"`

	Freshness time.Time `json:"freshness"`
}

// SetFreshness implements gathering.CanFressness.
func (d *DailyShopMetricData) SetFreshness(n time.Time) {
	d.Freshness = n
}

// Merge implements metric.MetricData.
func (d *DailyShopMetricData) Merge(dold interface{}) metric.MetricData {
	if dold == nil {
		return d
	}
	old := dold.(*DailyShopMetricData)

	d.AdsSpentAmount = old.AdsSpentAmount
	d.CancelOrderAmount = old.CancelOrderAmount

	d.ReturnCreatedAmount = old.ReturnCreatedAmount
	d.ReturnArrivedAmount = old.ReturnArrivedAmount

	d.ProblemOrderAmount = old.ProblemOrderAmount
	d.LostOrderAmount = old.LostOrderAmount

	d.CreatedOrderAmount = old.CreatedOrderAmount
	d.SysCreatedOrderAmount = old.SysCreatedOrderAmount
	d.EstWithdrawalAmount = old.EstWithdrawalAmount
	d.WithdrawalAmount = old.WithdrawalAmount
	d.MpAdjustmentAmount = old.MpAdjustmentAmount
	d.AdjOrderAmount = old.AdjOrderAmount
	return d
}

// var _ gathering.CanFressness = (*DailyShopMetricData)(nil)

func (d *DailyShopMetricData) Key() string {
	return fmt.Sprintf("metric/daily_shop/%s/%d/%d", d.Day, d.TeamID, d.ShopID)
}

type DailyShopPipeline struct {
	exact   exact_one.ExactlyOnce
	dmetric metric.MetricStore[*DailyShopMetricData]
	ctx     *yenstream.RunnerContext
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
		Via("dshop_only_backfill_update", yenstream.NewFlatMap(ds.ctx, func(cdata *stat_replica.CdcMessage) ([]*DailyShopMetricData, error) {
			data := cdata.Data.(*models.InvTransaction)
			result := []*DailyShopMetricData{}

			orddata := &models.InvOrderData{
				InvID: data.ID,
			}

			found, err := ds.exact.GetItemStruct(orddata)
			if err != nil {
				return result, err
			}

			if !found {
				return result, errors.New("order data not found")
			}

			switch cdata.ModType {
			case stat_replica.CdcBackfill:
				if data.Arrived.IsZero() {
					return result, nil
				}

				result = append(result, &DailyShopMetricData{
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
					result = append(result, &DailyShopMetricData{
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

			item := &DailyShopMetricData{
				Day:                 data.Created.Local().Format("2006-01-02"),
				ShopID:              orddata.MpID,
				TeamID:              orddata.TeamID,
				ReturnCreatedAmount: orddata.MpTotal,
			}

			err = ds.dmetric.Merge(item.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
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

			item := &DailyShopMetricData{
				Day:             data.Timestamp.Local().Format("2006-01-02"),
				ShopID:          ord.OrderMpID,
				TeamID:          ord.TeamID,
				LostOrderAmount: float64(ord.OrderMpTotal),
			}
			switch data.OrderStatus {
			case db_models.OrdProblem:
				ds.dmetric.Merge(item.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
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

			item := &DailyShopMetricData{
				Day:                data.Timestamp.Local().Format("2006-01-02"),
				ShopID:             ord.OrderMpID,
				TeamID:             ord.TeamID,
				ProblemOrderAmount: float64(ord.OrderMpTotal),
			}
			switch data.OrderStatus {
			case db_models.OrdProblem:
				ds.dmetric.Merge(item.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
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

			item := &DailyShopMetricData{
				Day:               data.Timestamp.Local().Format("2006-01-02"),
				ShopID:            ord.OrderMpID,
				TeamID:            ord.TeamID,
				CancelOrderAmount: float64(ord.OrderMpTotal),
			}
			switch data.OrderStatus {
			case db_models.OrdCancel:
				ds.dmetric.Merge(item.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
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

func NewShopDailyMetric(
	ctx *yenstream.RunnerContext,
	exact exact_one.ExactlyOnce,
	db *badger.DB,
	cdstream yenstream.Pipeline,
) (
	metric.MetricStore[*DailyShopMetricData],
	yenstream.Pipeline,
) {
	store := metric.NewDefaultMetricStore(db, func() *DailyShopMetricData {
		return &DailyShopMetricData{}
	}, func(data *DailyShopMetricData) *DailyShopMetricData {
		data.AdjOrderAmount = data.EstWithdrawalAmount - data.WithdrawalAmount
		return data
	})

	dayShopPipe := DailyShopPipeline{
		exact:   exact,
		dmetric: store,
		ctx:     ctx,
	}

	cancel := dayShopPipe.Cancel(cdstream)
	lost := dayShopPipe.Lost(cdstream)
	problem := dayShopPipe.Problem(cdstream)
	retur := dayShopPipe.Return(cdstream)

	withdrawal := cdstream.
		Via("filter_withdrawal", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "order_adjustments" {
				return false, nil
			}

			data := cdata.Data.(*models.OrderAdjustment)
			olddata := &models.OrderAdjustment{}

			if data.FundAt.IsZero() {
				return false, nil
			}
			var found bool
			err := exact.
				Change(data).
				Before(&found, olddata).
				Save().
				Err()

			cdata.OldData = olddata
			if !found {
				cdata.OldData = nil
			} else {
				if cdata.ModType == stat_replica.CdcBackfill {
					return false, err
				}
			}

			return true, err

		})).
		Via("process_withdrawal", yenstream.NewFlatMap(ctx, func(cdata *stat_replica.CdcMessage) ([]*DailyShopMetricData, error) {
			result := []*DailyShopMetricData{}

			data := cdata.Data.(*models.OrderAdjustment)

			// getting order
			ord := &models.Order{
				ID: data.OrderID,
			}

			// timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute*30)
			// defer cancel()

			found, err := exact.GetItemStruct(ord)
			if err != nil {
				return result, err
			}

			if !found {
				return result, errors.New("order not found")
			}

			item := &DailyShopMetricData{
				Day:                 data.FundAt.Local().Format("2006-01-02"),
				ShopID:              data.MpID,
				TeamID:              ord.TeamID,
				EstWithdrawalAmount: 0,
				WithdrawalAmount:    0,
				MpAdjustmentAmount:  0,
			}

			switch data.Type {
			case db_models.AdjOrderFund:
				item.EstWithdrawalAmount = float64(ord.OrderMpTotal)
				item.WithdrawalAmount = data.Amount
			default:
				item.MpAdjustmentAmount = data.Amount
			}

			result = append(result, item)

			if cdata.OldData != nil {
				olddata := cdata.OldData.(*models.OrderAdjustment)
				olditem := &DailyShopMetricData{
					Day:    olddata.FundAt.Local().Format("2006-01-02"),
					ShopID: olddata.MpID,
					TeamID: ord.TeamID,
				}

				switch data.Type {
				case db_models.AdjOrderFund:
					item.EstWithdrawalAmount = float64(ord.OrderMpTotal) * -1
					olditem.WithdrawalAmount = data.Amount * -1
				default:
					olditem.MpAdjustmentAmount = data.Amount * -1
				}
				result = append(result, olditem)
			}

			return result, nil
		})).
		Via("add_wd_to_metric_shop", yenstream.NewMap(ctx, func(data *DailyShopMetricData) (*DailyShopMetricData, error) {
			err := store.Merge(data.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
				if acc == nil {
					return data
				}
				acc.WithdrawalAmount += data.WithdrawalAmount
				acc.MpAdjustmentAmount += data.MpAdjustmentAmount
				acc.EstWithdrawalAmount += data.EstWithdrawalAmount
				return acc
			})
			return data, err
		}))

	order := cdstream.
		Via("process_orders", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "orders" {
				return false, nil
			}

			data := cdata.Data.(*models.Order)
			olddata := &models.Order{}
			var found bool
			err := exact.
				Change(data).
				Before(&found, olddata).
				Save().
				Err()

			cdata.OldData = olddata

			return true, err
		})).
		Via("add_order_to_metric_shop", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			data := cdata.Data.(*models.Order)
			day := data.OrderTime.Local().Format("2006-01-02")
			sysday := data.CreatedAt.Local().Format("2006-01-02")

			newitem := &DailyShopMetricData{
				ShopID:             data.OrderMpID,
				TeamID:             data.TeamID,
				Day:                day,
				CreatedOrderAmount: float64(data.OrderMpTotal),
			}

			sysitem := &DailyShopMetricData{
				ShopID:                data.OrderMpID,
				TeamID:                data.TeamID,
				Day:                   sysday,
				SysCreatedOrderAmount: float64(data.OrderMpTotal),
			}

			switch cdata.ModType {
			case stat_replica.CdcBackfill, stat_replica.CdcInsert:
				if cdata.OldData == nil {
					return cdata, nil
				}

				err = store.Merge(newitem.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
					if acc == nil {
						return newitem
					}
					acc.CreatedOrderAmount += newitem.CreatedOrderAmount
					return acc
				})

				if err != nil {
					return cdata, err
				}

				err = store.Merge(sysitem.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
					if acc == nil {
						return sysitem
					}
					acc.SysCreatedOrderAmount += sysitem.SysCreatedOrderAmount
					return acc
				})

				if err != nil {
					return cdata, err
				}

			}

			return cdata, nil
		}))

	// ads_expense_histories
	ads := cdstream.
		Via("process_ads", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "ads_expense_histories" {
				return false, nil
			}

			data := cdata.Data.(*models.AdsExpenseHistory)
			olddata := &models.AdsExpenseHistory{}
			var found bool
			err := exact.
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
		Via("add_metric_shop", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
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
				newitem := &DailyShopMetricData{
					ShopID:         data.MarketplaceID,
					TeamID:         data.TeamID,
					Day:            data.At.Local().Format("2006-01-02"),
					AdsSpentAmount: data.Amount,
				}

				err = store.Merge(newitem.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
					if acc == nil {
						return newitem
					}
					acc.AdsSpentAmount += newitem.AdsSpentAmount
					return acc
				})

				if err != nil {
					return cdata, err
				}

				olditem := &DailyShopMetricData{
					ShopID:         old.MarketplaceID,
					TeamID:         old.TeamID,
					Day:            old.At.Local().Format("2006-01-02"),
					AdsSpentAmount: old.Amount,
				}

				err = store.Merge(olditem.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
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

				newitem := &DailyShopMetricData{
					ShopID:         data.MarketplaceID,
					TeamID:         data.TeamID,
					Day:            data.At.Local().Format("2006-01-02"),
					AdsSpentAmount: 0,
				}

				err = store.Merge(newitem.Key(), func(acc *DailyShopMetricData) *DailyShopMetricData {
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

	result := yenstream.NewFlatten(ctx, "flatten shop daily metric",
		order,
		withdrawal,
		ads,
		cancel,
		lost,
		problem,
		retur,
	)
	return store, result
}
