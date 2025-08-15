package selling_metric

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/metric"
)

type DailyBankBalance struct {
	Day    string `gorm:"primaryKey" json:"day"`
	TeamID uint   `gorm:"primaryKey" json:"team_id"`

	ErrDiffAmount float64 `json:"err_diff_amount"`

	ActualDiffAmount float64 `json:"actual_diff_amount"`
	DiffAmount       float64 `json:"diff_amount"`

	// penambah
	WithdrawalAmount float64 `json:"withdrawal_amount"`
	CrossPaidAmount  float64 `json:"cross_paid_amount"`
	RefundAmount     float64 `json:"refund_amount"`

	// pengurang
	TopupSpayAmount    float64 `json:"topup_spay_amount"`
	OngkirCodAmount    float64 `json:"ongkir_cod_amount"`
	WarehouseFeeAmount float64 `json:"warehouse_fee_amount"`
	AdsCostAmount      float64 `json:"ads_cost_amount"`
	CrossCostAmount    float64 `json:"cross_cost_amount"`
	RestockCostAmount  float64 `json:"restock_cost_amount"`
	AdjAmount          float64 `json:"adj_amount"`

	Freshness time.Time
}

// SetFreshness implements gathering.CanFressness.
func (d *DailyBankBalance) SetFreshness(n time.Time) {
	d.Freshness = n
}

// Key implements metric.MetricData.
func (d *DailyBankBalance) Key() string {
	return fmt.Sprintf("metric/daily_bank/%s/%d", d.Day, d.TeamID)
}

// Merge implements metric.MetricData.
func (d *DailyBankBalance) Merge(dold interface{}) metric.MetricData {
	if dold == nil {
		return d
	}
	old := dold.(*DailyBankBalance)

	d.ErrDiffAmount += old.ErrDiffAmount

	d.ActualDiffAmount += old.ActualDiffAmount
	d.DiffAmount += old.DiffAmount

	// penambah
	d.WithdrawalAmount += old.WithdrawalAmount
	d.CrossPaidAmount += old.CrossPaidAmount
	d.RefundAmount += old.RefundAmount

	// pengurang
	d.TopupSpayAmount += old.TopupSpayAmount
	d.OngkirCodAmount += old.OngkirCodAmount
	d.WarehouseFeeAmount += old.WarehouseFeeAmount
	d.AdsCostAmount += old.AdsCostAmount
	d.CrossCostAmount += old.CrossCostAmount
	d.RestockCostAmount += old.RestockCostAmount
	d.AdjAmount += old.AdjAmount

	return d
}

func NewDailyBankBalance(badgedb *badger.DB) metric.MetricStore[*DailyBankBalance] {
	return metric.NewDefaultMetricStore(
		badgedb,
		func() *DailyBankBalance {
			return &DailyBankBalance{}
		},
		func(dsb *DailyBankBalance) *DailyBankBalance {
			return dsb
		},
	)
}
