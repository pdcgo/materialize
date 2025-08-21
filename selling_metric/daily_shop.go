package selling_metric

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
)

type DailyShopMetricData struct {
	Day               string  `json:"day" gorm:"primaryKey"`
	ShopID            uint    `json:"shop_id" gorm:"primaryKey"`
	TeamID            uint    `json:"team_id"`
	AdsSpentAmount    float64 `json:"ads_spent_amount"`
	CancelOrderAmount float64 `json:"cancel_order_amount"`

	ReturnCreatedAmount float64 `json:"return_created_amount"`
	ReturnArrivedAmount float64 `json:"return_arrived_amount"`

	ProblemOrderAmount float64 `json:"problem_order_amount"`
	LostOrderAmount    float64 `json:"lost_order_amount"`

	CreatedOrderAmount    float64 `json:"created_order_amount"`
	SysCreatedOrderAmount float64 `json:"sys_created_order_amount"`

	EstWithdrawalAmount float64 `json:"est_withdrawal_amount"`
	WithdrawalAmount    float64 `json:"withdrawal_amount"`
	MpAdjustmentAmount  float64 `json:"mp_adjustment_amount"`
	AdjOrderAmount      float64 `json:"adj_order_amount"`

	WarehouseFeeAmount float64 `json:"warehouse_fee_amount"`

	Freshness time.Time `json:"freshness"`
}

// SetFreshness implements gathering.CanFressness.
func (d *DailyShopMetricData) SetFreshness(n time.Time) {
	d.Freshness = n
}

// Merge implements metric.MetricData.
func (d *DailyShopMetricData) Merge(dold interface{}) metric.MetricData {
	if dold == nil {
		return d
	}
	old := dold.(*DailyShopMetricData)

	d.AdsSpentAmount += old.AdsSpentAmount
	d.CancelOrderAmount += old.CancelOrderAmount

	d.ReturnCreatedAmount += old.ReturnCreatedAmount
	d.ReturnArrivedAmount += old.ReturnArrivedAmount

	d.ProblemOrderAmount += old.ProblemOrderAmount
	d.LostOrderAmount += old.LostOrderAmount

	d.CreatedOrderAmount += old.CreatedOrderAmount
	d.SysCreatedOrderAmount += old.SysCreatedOrderAmount
	d.EstWithdrawalAmount += old.EstWithdrawalAmount
	d.WithdrawalAmount += old.WithdrawalAmount
	d.MpAdjustmentAmount += old.MpAdjustmentAmount
	d.AdjOrderAmount += old.AdjOrderAmount
	d.WarehouseFeeAmount += old.WarehouseFeeAmount
	return d
}

// var _ gathering.CanFressness = (*DailyShopMetricData)(nil)

func (d *DailyShopMetricData) Key() string {
	return fmt.Sprintf("metric/daily_shop/%s/%d/%d", d.Day, d.TeamID, d.ShopID)
}

func NewDailyShopMetric(
	badgedb *badger.DB,
	exact exact_one.ExactlyOnce,
) metric.MetricStore[*DailyShopMetricData] {
	met := metric.NewDefaultMetricStore(badgedb, func() *DailyShopMetricData {
		return &DailyShopMetricData{}
	}, func(data *DailyShopMetricData) *DailyShopMetricData {
		data.AdjOrderAmount = data.EstWithdrawalAmount - data.WithdrawalAmount
		return data
	})

	return met
}
