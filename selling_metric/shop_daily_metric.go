package selling_metric

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

type DailyShopMetricData struct {
	Day      string  `json:"day" gorm:"primaryKey"`
	ShopID   uint    `json:"shop_id" gorm:"primaryKey"`
	TeamID   uint    `json:"team_id"`
	AdsSpent float64 `json:"ads_spent"`
}

func (d *DailyShopMetricData) Key() string {
	return fmt.Sprintf("metric/daily_shop/%s/%d/%d", d.Day, d.TeamID, d.ShopID)
}

func NewShopDailyMetric(
	ctx *yenstream.RunnerContext,
	db *badger.DB,
	cdstream yenstream.Pipeline,
) (
	metric.MetricStore[*DailyShopMetricData],
	yenstream.Pipeline,
) {
	store := metric.NewDefaultMetricStore(db, func() *DailyShopMetricData {
		return &DailyShopMetricData{}
	}, nil)

	// ads_expense_histories

	return store, cdstream
}
