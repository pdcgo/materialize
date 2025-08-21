package selling_pipeline

import (
	"fmt"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/yenstream"
)

// warehouse fee
// cross cost
// stock cost
func (ds *DailyShopPipeline) WarehouseFee(cdstream yenstream.Pipeline) yenstream.Pipeline {
	ware := cdstream.
		Via("dshop_warehouse", yenstream.NewFilter(ds.ctx,
			func(cdata *stat_replica.CdcMessage) (bool, error) {
				if cdata.SourceMetadata.Table != "inv_transactions" {
					return false, nil
				}

				data := cdata.Data.(*models.InvTransaction)
				if data.Type != db_models.InvTxOrder {
					return false, nil
				}

				return true, nil
			})).
		Via("dshop_warehouse_flat", yenstream.NewFlatMap(ds.ctx,
			func(cdata *stat_replica.CdcMessage) ([]*selling_metric.DailyShopMetricData, error) {
				var err error
				var result []*selling_metric.DailyShopMetricData

				data := cdata.Data.(*models.InvTransaction)
				invord := &models.InvOrderData{
					InvID: data.ID,
				}

				var found bool
				found, err = ds.exact.GetItemStructKey(invord.Key(), invord)
				if err != nil {
					return result, err
				}

				if !found {
					return result, fmt.Errorf("order not found in inv %d", data.ID)
				}

				switch cdata.ModType {
				case stat_replica.CdcInsert:
					item := &selling_metric.DailyShopMetricData{
						Day:                data.Created.Local().Format("2006-01-02"),
						TeamID:             data.TeamID,
						ShopID:             invord.MpID,
						WarehouseFeeAmount: invord.WarehouseFee,
					}
					result = append(result, item)

				case stat_replica.CdcBackfill:
					if cdata.OldData != nil {
						return result, err
					}

					if data.Status == db_models.InvTxCancel {
						return result, nil
					}

					item := &selling_metric.DailyShopMetricData{
						Day:                data.Created.Local().Format("2006-01-02"),
						TeamID:             data.TeamID,
						ShopID:             invord.MpID,
						WarehouseFeeAmount: invord.WarehouseFee,
					}
					result = append(result, item)

				case stat_replica.CdcUpdate:
					if cdata.OldData == nil {
						item := &selling_metric.DailyShopMetricData{
							Day:                data.Created.Local().Format("2006-01-02"),
							TeamID:             data.TeamID,
							ShopID:             invord.MpID,
							WarehouseFeeAmount: invord.WarehouseFee,
						}
						result = append(result, item)
						return result, nil
					}

					olddata := cdata.OldData.(*models.InvTransaction)
					if olddata.Status != db_models.InvTxCancel {
						if data.Status == db_models.InvTxCancel {
							item := &selling_metric.DailyShopMetricData{
								Day:                data.Created.Local().Format("2006-01-02"),
								TeamID:             data.TeamID,
								ShopID:             invord.MpID,
								WarehouseFeeAmount: invord.WarehouseFee * -1,
							}
							result = append(result, item)

							return result, nil
						}
					}

				}

				return result, nil
			})).
		Via("dshop_ware_merge", yenstream.NewMap(ds.ctx,
			func(met *selling_metric.DailyShopMetricData) (*selling_metric.DailyShopMetricData, error) {
				err := ds.metric.Merge(met.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
					if acc == nil {
						return met
					}

					acc.Merge(met)
					return acc
				})

				return met, err
			}))

	return ware
}
