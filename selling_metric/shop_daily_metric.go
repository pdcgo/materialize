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
	Day                   string  `json:"day" gorm:"primaryKey"`
	ShopID                uint    `json:"shop_id" gorm:"primaryKey"`
	TeamID                uint    `json:"team_id"`
	AdsSpentAmount        float64 `json:"ads_spent_amount"`
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

// var _ gathering.CanFressness = (*DailyShopMetricData)(nil)

func (d *DailyShopMetricData) Key() string {
	return fmt.Sprintf("metric/daily_shop/%s/%d/%d", d.Day, d.TeamID, d.ShopID)
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

	cancel := cdstream.
		Via("order_timestamps", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			return true, nil
		}))

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
	)
	return store, result
}
