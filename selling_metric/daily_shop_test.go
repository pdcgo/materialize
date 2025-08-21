package selling_metric_test

import (
	"testing"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/stretchr/testify/assert"
)

func TestDailyShopMerge(t *testing.T) {
	acc := &selling_metric.DailyShopMetricData{}

	met := &selling_metric.DailyShopMetricData{
		WarehouseFeeAmount: 5000,
	}
	acc.Merge(met)

	assert.Equal(t, 5000.00, acc.WarehouseFeeAmount)
}

func TestMatric(t *testing.T) {
	var bdb db_mock.BadgeDBMock

	moretest.Suite(t, "test matric",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&bdb),
		},
		func(t *testing.T) {
			exact := exact_one.NewBadgeExactOne(t.Context(), bdb.DB)
			mat := selling_metric.NewDailyShopMetric(bdb.DB, exact)

			matd := &selling_metric.DailyShopMetricData{
				Day:                "2025-08-01",
				ShopID:             1,
				TeamID:             1,
				CreatedOrderAmount: 12000,
			}

			err := mat.Merge(matd.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
				if acc == nil {
					return matd
				}

				acc.CreatedOrderAmount += matd.CreatedOrderAmount
				return acc
			})
			assert.Nil(t, err)

			err = mat.FlushCallback(func(acc any) error {
				// t.Error(acc)
				return nil
			})
			assert.Nil(t, err)

			err = mat.Merge(matd.Key(), func(acc *selling_metric.DailyShopMetricData) *selling_metric.DailyShopMetricData {
				if acc == nil {
					return matd
				}

				acc.CreatedOrderAmount += matd.CreatedOrderAmount
				return acc
			})
			assert.Nil(t, err)

			err = mat.FlushCallback(func(acc any) error {
				// t.Error(acc)
				return nil
			})
			assert.Nil(t, err)
		},
	)

}
