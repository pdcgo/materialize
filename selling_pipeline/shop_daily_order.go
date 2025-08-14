package selling_pipeline

import (
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/yenstream"
)

func (ds *DailyShopPipeline) Order(cdstream yenstream.Pipeline) yenstream.Pipeline {
	order := cdstream.
		Via("process_orders", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "orders" {
				return false, nil
			}

			return true, nil
		})).
		Via("add_order_to_metric_shop", yenstream.NewMap(ds.ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {

			var err error
			data := cdata.Data.(*models.Order)
			day := data.OrderTime.Local().Format("2006-01-02")
			sysday := data.CreatedAt.Local().Format("2006-01-02")

			newitem := &selling_metric.DailyShopMetricData{
				ShopID:             data.OrderMpID,
				TeamID:             data.TeamID,
				Day:                day,
				CreatedOrderAmount: float64(data.OrderMpTotal),
			}

			sysitem := &selling_metric.DailyShopMetricData{
				ShopID:                data.OrderMpID,
				TeamID:                data.TeamID,
				Day:                   sysday,
				SysCreatedOrderAmount: float64(data.OrderMpTotal),
			}

			switch cdata.ModType {
			case stat_replica.CdcBackfill, stat_replica.CdcInsert:
				if cdata.OldData != nil {
					return cdata, nil
				}

				err = ds.metric.Merge(newitem.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return newitem
					}
					acc.CreatedOrderAmount += newitem.CreatedOrderAmount
					return acc
				})

				if err != nil {
					return cdata, err
				}

				err = ds.metric.Merge(sysitem.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
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

	return order
}
