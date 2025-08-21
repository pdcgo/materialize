package selling_pipeline_test

import (
	"testing"
	"time"

	"github.com/pdcgo/materialize/debug_pipeline"
	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/yenstream"
	"github.com/stretchr/testify/assert"
)

func TestWarehouse(t *testing.T) {
	ctx := t.Context()

	var bdb db_mock.BadgeDBMock
	cdchan := make(chan *stat_replica.CdcMessage, 1)

	go func() {
		defer close(cdchan)
		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "orders",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.Order{
				ID:            1,
				TeamID:        1,
				WarehouseFee:  5000,
				OrderMpTotal:  12000,
				InvertoryTxID: 1,
				CreatedAt:     time.Now(),
				OrderTime:     time.Now().AddDate(0, 0, -2),
			},
		}
		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "inv_transactions",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.InvTransaction{
				ID:          1,
				TeamID:      1,
				WarehouseID: 1,
				Type:        db_models.InvTxOrder,
				Status:      db_models.InvTxOngoing,
				Created:     time.Now(),
			},
		}
	}()

	moretest.Suite(t, "test withdrawal",
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
							return data, nil
						})))

					return selling_metric.
						NewMetricStream(ctx, time.Second*5, met, order).
						DataChanges(bdb.DB).
						Via("debug", debug_pipeline.Log(ctx)).
						Via("testing", yenstream.NewMap(ctx, func(data *selling_metric.DailyShopMetricData) (*selling_metric.DailyShopMetricData, error) {

							switch data.Day {
							case time.Now().Format("2006-01-02"):
								c += 1
								assert.Equal(t, 5000.00, data.WarehouseFeeAmount)
							}
							return data, nil
						}))

				})

			assert.Equal(t, 1, c)
		},
	)
}
