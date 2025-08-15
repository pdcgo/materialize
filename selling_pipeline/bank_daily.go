package selling_pipeline

import (
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

// var _ metric.MetricData = (*selling_metric.DailyBankBalance)(nil)

type DailyBankPipeline struct {
	ctx    *yenstream.RunnerContext
	metric metric.MetricStore[*selling_metric.DailyBankBalance]
}

func (dbk *DailyBankPipeline) All(
	dailyTeam yenstream.Pipeline,

) yenstream.Pipeline {

	return dailyTeam.
		Via("merge_withdrawal", yenstream.NewMap(dbk.ctx,
			func(data *selling_metric.DailyTeamMetricData) (*selling_metric.DailyBankBalance, error) {
				md := selling_metric.DailyBankBalance{
					Day:              data.Day,
					TeamID:           data.TeamID,
					WithdrawalAmount: data.WithdrawalAmount,
				}
				item := md
				err := dbk.metric.Merge(md.Key(), func(acc *selling_metric.DailyBankBalance) *selling_metric.DailyBankBalance {
					if acc == nil {
						return &item
					}

					acc.WithdrawalAmount += item.WithdrawalAmount
					return acc
				})

				return &md, err
			}))
}

func NewDailyBankPipeline(
	ctx *yenstream.RunnerContext,
	met metric.MetricStore[*selling_metric.DailyBankBalance],
) *DailyBankPipeline {
	return &DailyBankPipeline{
		ctx:    ctx,
		metric: met,
	}
}
