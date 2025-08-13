package selling_pipeline_test

import (
	"testing"
	"time"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/yenstream"
	"github.com/stretchr/testify/assert"
)

func TestShopeepay(t *testing.T) {
	var bdb db_mock.BadgeDBMock
	cdchan := make(chan *stat_replica.CdcMessage, 1)

	ctx := t.Context()

	go func() {
		defer close(cdchan)
		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "expense_accounts",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.ExpenseAccount{
				ID:            1,
				TeamID:        1,
				AccountTypeID: 7,
			},
		}
		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "balance_account_histories",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.BalanceAccountHistory{
				ID:        1,
				TeamID:    1,
				AccountID: 1,
				Amount:    10000,
				At:        time.Now().Local().AddDate(0, 0, -1),
				CreatedAt: time.Now().Local(),
			},
		}
		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "balance_account_histories",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.BalanceAccountHistory{
				ID:        1,
				TeamID:    1,
				AccountID: 1,
				Amount:    11000,
				At:        time.Now().Local(),
				CreatedAt: time.Now().Local(),
			},
		}

		cdchan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "balance_account_histories",
				Schema: "public",
			},
			ModType: stat_replica.CdcUpdate,
			Data: &models.BalanceAccountHistory{
				ID:        1,
				TeamID:    1,
				AccountID: 1,
				Amount:    12000,
				At:        time.Now().Local(),
				CreatedAt: time.Now().Local(),
			},
		}

	}()

	moretest.Suite(t, "testing shopeepay balance",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&bdb),
		},
		func(t *testing.T) {
			var c int = 0

			exact := exact_one.NewBadgeExactOne(ctx, bdb.DB)

			smetric := selling_metric.NewDailyShopeepayBalanceMetric(bdb.DB, exact)

			yenstream.NewRunnerContext(ctx).
				CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
					source := yenstream.
						NewChannelSource(ctx, cdchan).
						Via("map", yenstream.NewFilter(ctx, func(data any) (bool, error) {
							return true, nil
						}))

					source = selling_pipeline.
						ExactOne(ctx, exact, source)

					pipe := selling_pipeline.NewDailyShopeepayPipeline(ctx, bdb.DB, smetric, exact)
					all := pipe.All(source)

					smetpipe := all.
						Via("metric shopee", selling_metric.NewMetricStream(ctx, time.Second, smetric)).
						Via("testing stream", yenstream.NewMap(ctx, func(met *metric.DailyShopeepayBalance) (*metric.DailyShopeepayBalance, error) {

							c += 1
							if met.Day == time.Now().Local().Format("2006-01-02") {
								assert.Equal(t, 2000.00, met.ActualDiffAmount)
							}
							return met, nil
						}))

					return yenstream.NewFlatten(ctx, "flatall", smetpipe)
				})

			assert.Equal(t, 1, c)
		},
	)
}
