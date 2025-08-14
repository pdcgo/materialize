package selling_pipeline_test

import (
	"log"
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

func TestCreateOrder(t *testing.T) {
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
			ModType: stat_replica.CdcInsert,
			Data: &models.Order{
				ID:           1,
				OrderMpTotal: 12000,
				CreatedAt:    time.Now(),
				OrderTime:    time.Now().AddDate(0, 0, -1),
			},
		}

		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "orders",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.Order{
				ID:           1,
				OrderMpTotal: 12000,
				CreatedAt:    time.Now(),
				OrderTime:    time.Now().AddDate(0, 0, -1),
			},
		}

		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "orders",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.Order{
				ID:           1,
				OrderMpTotal: 12000,
				CreatedAt:    time.Now(),
				OrderTime:    time.Now().AddDate(0, 0, -1),
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
						Order(source.Via("test", yenstream.NewMap(ctx, func(data any) (any, error) {
							return data, nil
						})))

					return order.
						Via("get_stream", selling_metric.NewMetricDataStream(ctx, time.Second, met)).
						Via("testing", yenstream.NewMap(ctx, func(data *selling_metric.DailyShopMetricData) (*selling_metric.DailyShopMetricData, error) {
							c += 1

							switch data.Day {
							case time.Now().Format("2006-01-02"):
								assert.Equal(t, 12000.00, data.SysCreatedOrderAmount)
							case time.Now().AddDate(0, 0, -1).Format("2006-01-02"):
								assert.Equal(t, 12000.00, data.CreatedOrderAmount)
							}

							log.Println(data, "asdasdasd")
							return data, nil
						}))
				})

			assert.Equal(t, 2, c)
		},
	)

}
