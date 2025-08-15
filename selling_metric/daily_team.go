package selling_metric

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
)

type DailyTeamMetricData struct {
	Day               string  `json:"day" gorm:"primaryKey"`
	TeamID            uint    `json:"team_id" gorm:"primaryKey"`
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

	Freshness time.Time `json:"freshness"`
}

// SetFreshness implements gathering.CanFressness.
func (d *DailyTeamMetricData) SetFreshness(n time.Time) {
	d.Freshness = n
}

// Merge implements metric.MetricData.
func (d *DailyTeamMetricData) Merge(dold interface{}) metric.MetricData {
	if dold == nil {
		return d
	}
	old := dold.(*DailyTeamMetricData)

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
	return d
}

// var _ gathering.CanFressness = (*DailyShopMetricData)(nil)

func (d *DailyTeamMetricData) Key() string {
	return fmt.Sprintf("metric/daily_team/%s/%d", d.Day, d.TeamID)
}

func NewDailyTeamMetric(
	badgedb *badger.DB,
	exact exact_one.ExactlyOnce,
) metric.MetricStore[*DailyTeamMetricData] {
	met := metric.NewDefaultMetricStore(badgedb, func() *DailyTeamMetricData {
		return &DailyTeamMetricData{}
	}, func(data *DailyTeamMetricData) *DailyTeamMetricData {
		data.AdjOrderAmount = data.EstWithdrawalAmount - data.WithdrawalAmount
		return data
	})

	return met
}
