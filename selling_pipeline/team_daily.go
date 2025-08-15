package selling_pipeline

import (
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

type DailyTeamPipeline struct {
	ctx    *yenstream.RunnerContext
	metric metric.MetricStore[*selling_metric.DailyTeamMetricData]
}

func (dt *DailyTeamPipeline) All(dailyShop yenstream.Pipeline) yenstream.Pipeline {
	dailyTeam := dailyShop.
		Via("merge_to_daily_team", yenstream.NewMap(dt.ctx, func(md *selling_metric.DailyShopMetricData) (*selling_metric.DailyTeamMetricData, error) {
			dd := selling_metric.DailyTeamMetricData{
				Day:                   md.Day,
				TeamID:                md.TeamID,
				AdsSpentAmount:        md.AdsSpentAmount,
				CancelOrderAmount:     md.CancelOrderAmount,
				ReturnCreatedAmount:   md.ReturnCreatedAmount,
				ReturnArrivedAmount:   md.ReturnArrivedAmount,
				ProblemOrderAmount:    md.ProblemOrderAmount,
				LostOrderAmount:       md.LostOrderAmount,
				CreatedOrderAmount:    md.CreatedOrderAmount,
				SysCreatedOrderAmount: md.SysCreatedOrderAmount,
				EstWithdrawalAmount:   md.EstWithdrawalAmount,
				WithdrawalAmount:      md.WithdrawalAmount,
				MpAdjustmentAmount:    md.MpAdjustmentAmount,
				AdjOrderAmount:        md.AdjOrderAmount,
			}

			item := dd
			err := dt.metric.Merge(item.Key(), func(acc *selling_metric.DailyTeamMetricData) *selling_metric.DailyTeamMetricData {
				if acc == nil {
					return &item
				}

				acc.Merge(&item)
				return acc
			})
			return &dd, err
		}))
		// Via("test", yenstream.NewMap(dt.ctx,
		// 	func(met *selling_metric.DailyTeamMetricData) (*selling_metric.DailyTeamMetricData, error) {
		// 		// if met.TeamID == 31 && met.Day == "2025-08-15" {
		// 		// 	log.Println(met, met.WithdrawalAmount)
		// 		// }
		// 		return met, nil
		// 	}))

	return dailyTeam
}

func NewDailyTeamPipeline(
	ctx *yenstream.RunnerContext,
	metric metric.MetricStore[*selling_metric.DailyTeamMetricData],

) *DailyTeamPipeline {
	return &DailyTeamPipeline{
		ctx:    ctx,
		metric: metric,
	}
}
