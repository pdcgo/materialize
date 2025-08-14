package selling_pipeline_test

import (
	"testing"
	"time"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/yenstream"
	"github.com/stretchr/testify/assert"
)

func TestDailyTeamStream(t *testing.T) {
	var bdb db_mock.BadgeDBMock

	dailyShopChan := make(chan *selling_metric.DailyShopMetricData, 1)
	go func() {
		defer close(dailyShopChan)

		dailyShopChan <- &selling_metric.DailyShopMetricData{
			Day:            "2025-08-14",
			ShopID:         1,
			TeamID:         1,
			AdsSpentAmount: 20000,
		}
		// dailyShopChan <- &selling_metric.DailyShopMetricData{
		// 	Day:               "2025-08-14",
		// 	ShopID:            1,
		// 	TeamID:            1,
		// 	CancelOrderAmount: 30000,
		// }
	}()

	moretest.Suite(t, "test daily stream",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&bdb),
		},
		func(t *testing.T) {
			ctx := t.Context()

			exact := exact_one.NewBadgeExactOne(ctx, bdb.DB)
			teamMetric := selling_metric.NewDailyTeamMetric(bdb.DB, exact)

			c := 0
			d := 0

			yenstream.
				NewRunnerContext(ctx).
				CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
					source := yenstream.NewChannelSource(ctx, dailyShopChan).
						Via("dump", yenstream.NewMap(ctx, func(data any) (any, error) {
							return data, nil
						}))

					teamPipe := selling_pipeline.
						NewDailyTeamPipeline(
							ctx,
							teamMetric,
						).
						All(source).
						Via("testing", yenstream.NewMap(ctx, func(data *selling_metric.DailyTeamMetricData) (*selling_metric.DailyTeamMetricData, error) {
							c += 1
							if data.Day == "2025-08-14" {
								assert.Equal(t, 20000.00, data.AdsSpentAmount)
							}

							return data, nil
						}))

					teamStream := selling_metric.
						NewMetricStream(ctx, time.Second, teamMetric, teamPipe)

					teamchanges := teamStream.
						CounterChanges().
						Via("testing", yenstream.NewMap(ctx, func(data *selling_metric.DailyTeamMetricData) (*selling_metric.DailyTeamMetricData, error) {
							d += 1
							if data.Day == "2025-08-14" {
								assert.Equal(t, 20000.00, data.AdsSpentAmount)
							}
							return data, nil
						}))

					teamdata := teamStream.
						DataChanges(bdb.DB).
						Via("test not empty", yenstream.NewMap(ctx, func(data *selling_metric.DailyTeamMetricData) (*selling_metric.DailyTeamMetricData, error) {
							assert.NotEmpty(t, data)
							if data.Day == "2025-08-14" {
								assert.Equal(t, 20000.00, data.AdsSpentAmount)
							}
							return data, nil
						}))

					return yenstream.NewFlatten(ctx, "flattenstream",
						teamchanges,
						teamdata,
					)
				})

			assert.Equal(t, 1, c)
			assert.Equal(t, 1, d)
		},
	)
}
