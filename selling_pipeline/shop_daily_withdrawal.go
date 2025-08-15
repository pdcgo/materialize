package selling_pipeline

import (
	"errors"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

func (ds *DailyShopPipeline) Withdrawal(cdstream yenstream.Pipeline) yenstream.Pipeline {
	withdrawal := cdstream.
		Via("filter_withdrawal", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "order_adjustments" {
				return false, nil
			}

			data := cdata.Data.(*models.OrderAdjustment)

			if data.FundAt.IsZero() {
				return false, nil
			}
			var found bool
			if cdata.OldData != nil {
				// olddata = cdata.OldData.(*models.OrderAdjustment)
				found = true
			}

			if found {
				if cdata.ModType == stat_replica.CdcBackfill {
					return false, nil
				}
			}

			return true, nil

		})).
		Via("process_withdrawal", yenstream.NewFlatMap(ds.ctx, func(cdata *stat_replica.CdcMessage) ([]*selling_metric.DailyShopMetricData, error) {
			result := []*selling_metric.DailyShopMetricData{}

			data := cdata.Data.(*models.OrderAdjustment)

			// getting order
			ord := &models.Order{
				ID: data.OrderID,
			}

			// timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute*30)
			// defer cancel()

			found, err := ds.exact.GetItemStruct(ord)
			if err != nil {
				return result, err
			}

			if !found {
				return result, errors.New("order not found")
			}

			item := &selling_metric.DailyShopMetricData{
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
				olditem := &selling_metric.DailyShopMetricData{
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
		Via("add_wd_to_metric_shop", yenstream.NewMap(ds.ctx, func(data *selling_metric.DailyShopMetricData) (*selling_metric.DailyShopMetricData, error) {
			err := ds.metric.Merge(data.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
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
	return withdrawal
}
