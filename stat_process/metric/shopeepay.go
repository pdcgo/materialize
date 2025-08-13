package metric

import (
	"fmt"
	"time"
)

// var _ MetricData = (*DailyShopeePayBalance)(nil)

type DailyShopeepayBalance struct {
	Day              string    `json:"day" gorm:"primaryKey"`
	TeamID           uint      `json:"team_id" gorm:"primaryKey"`
	TeamName         string    `json:"team_name"`
	DiffAmount       float64   `json:"diff_amount"`
	ErrDiffAmount    float64   `json:"err_diff_amount"`
	ActualDiffAmount float64   `json:"actual_diff_amount"`
	RefundAmount     float64   `json:"refund_amount"`
	CostAmount       float64   `json:"cost_amount"`
	TopupAmount      float64   `json:"topup_amount"`
	Freshness        time.Time `json:"-"`
}

// Merge implements MetricData.
func (d *DailyShopeepayBalance) Merge(dold interface{}) MetricData {
	if dold == nil {
		return d
	}

	old := dold.(*DailyShopeepayBalance)

	return &DailyShopeepayBalance{
		Day:              d.Day,
		TeamID:           d.TeamID,
		TeamName:         d.TeamName,
		DiffAmount:       d.DiffAmount + old.DiffAmount,
		ErrDiffAmount:    d.ErrDiffAmount + old.ErrDiffAmount,
		ActualDiffAmount: d.ActualDiffAmount + old.ActualDiffAmount,
		RefundAmount:     d.RefundAmount + old.RefundAmount,
		CostAmount:       d.CostAmount + old.CostAmount,
		TopupAmount:      d.TopupAmount + old.TopupAmount,
	}
}

func (d *DailyShopeepayBalance) SetFreshness(n time.Time) {
	d.Freshness = n
}

func (d *DailyShopeepayBalance) Merges(old *DailyShopeepayBalance) *DailyShopeepayBalance {
	if old == nil {
		return d
	}

	return &DailyShopeepayBalance{
		Day:              d.Day,
		TeamID:           d.TeamID,
		TeamName:         d.TeamName,
		DiffAmount:       d.DiffAmount + old.DiffAmount,
		ErrDiffAmount:    d.ErrDiffAmount + old.ErrDiffAmount,
		ActualDiffAmount: d.ActualDiffAmount + old.ActualDiffAmount,
		RefundAmount:     d.RefundAmount + old.RefundAmount,
		CostAmount:       d.CostAmount + old.CostAmount,
		TopupAmount:      d.TopupAmount + old.TopupAmount,
	}
}

// CsvData implements main.CsvItem.
func (d *DailyShopeepayBalance) CsvData() ([]string, error) {
	return []string{
		fmt.Sprintf("%d", d.TeamID),
		d.Day,
		d.TeamName,
		fmt.Sprintf("%.3f", d.ErrDiffAmount),
		fmt.Sprintf("%.3f", d.DiffAmount),
		fmt.Sprintf("%.3f", d.ActualDiffAmount),
		fmt.Sprintf("%.3f", d.RefundAmount),
		fmt.Sprintf("%.3f", d.CostAmount),
		fmt.Sprintf("%.3f", d.TopupAmount),
	}, nil
}

// CsvHeaders implements main.CsvItem.
func (d *DailyShopeepayBalance) CsvHeaders() []string {
	return []string{
		"team_id",
		"day",
		"team_name",
		"balancing",
		"selisih_web",
		"selisih_shopeepay",
		"refund",
		"cost",
		"topup",
	}
}

// Key implements MetricData.
func (d *DailyShopeepayBalance) Key() string {
	return fmt.Sprintf("metric/daily_shopeepay_balance/%s/%d", d.Day, d.TeamID)
}
