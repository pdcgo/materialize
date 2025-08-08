package metric

import "fmt"

type DailyWarehouseInvoice struct {
	WarehouseID   uint    `json:"warehouse_id"`
	Day           string  `json:"day"`
	CreatedAmount float64 `json:"created_amount"`
}

// Key implements MetricData.
func (d *DailyWarehouseInvoice) Key() string {
	return fmt.Sprintf("metric/daily_warehouse/%s/%d", d.Day, d.WarehouseID)
}
