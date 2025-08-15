package selling_pipeline_test

import (
	"testing"
	"time"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/yenstream"
	"github.com/stretchr/testify/assert"
)

func TestAds(t *testing.T) {
	ctx := t.Context()

	var bdb db_mock.BadgeDBMock
	cdchan := make(chan *stat_replica.CdcMessage, 1)

	go func() {
		defer close(cdchan)
		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "ads_expense_histories",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.AdsExpenseHistory{
				ID:            1,
				TeamID:        1,
				CreatedByID:   1,
				MarketplaceID: 1,
				Amount:        0,
				At:            time.Now(),
				CreatedAt:     time.Now(),
			},
		}

		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "ads_expense_histories",
				Schema: "public",
			},
			ModType: stat_replica.CdcUpdate,
			Data: &models.AdsExpenseHistory{
				ID:            1,
				TeamID:        1,
				CreatedByID:   1,
				MarketplaceID: 1,
				Amount:        9000,
				At:            time.Now(),
				CreatedAt:     time.Now(),
			},
		}

	}()

	moretest.Suite(t, "test create order",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&bdb),
		},
		func(t *testing.T) {
			exact := exact_one.NewBadgeExactOne(ctx, bdb.DB)
			met := selling_metric.NewDailyShopMetric(bdb.DB, exact)

			c := 0

			yenstream.
				NewRunnerContext(ctx).
				CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {

					source := yenstream.
						NewChannelSource(ctx, cdchan).
						Via("map", yenstream.NewFilter(ctx, func(data any) (bool, error) {
							return true, nil
						}))

					source = selling_pipeline.
						ExactOne(ctx, exact, source)

					order := selling_pipeline.
						NewShopDailyPipeline(ctx, bdb.DB, met, exact).
						All(source.Via("test", yenstream.NewMap(ctx, func(data any) (any, error) {
							c += 1
							return data, nil
						})))

					return selling_metric.
						NewMetricStream(ctx, time.Second, met, order).
						DataChanges(bdb.DB).
						Via("testing", yenstream.NewMap(ctx, func(data *selling_metric.DailyShopMetricData) (*selling_metric.DailyShopMetricData, error) {

							switch data.Day {
							case time.Now().Format("2006-01-02"):
								assert.Equal(t, 9000.00, data.AdsSpentAmount)
							}

							return data, nil
						}))

				})

			assert.Equal(t, 2, c)
		},
	)
}
