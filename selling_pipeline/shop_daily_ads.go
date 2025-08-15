package selling_pipeline

import (
	"errors"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/yenstream"
)

func (ds *DailyShopPipeline) Ads(cdstream yenstream.Pipeline) yenstream.Pipeline {
	ads := cdstream.
		Via("process_ads", yenstream.NewFilter(ds.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "ads_expense_histories" {
				return false, nil
			}
			return true, nil
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
