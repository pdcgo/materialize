package models

import (
	"fmt"
	"time"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
)

type InvResolution struct {
	ID          uint `json:"id"`
	TxID        uint `json:"tx_id"`
	TeamID      uint `json:"team_id"`
	WarehouseID uint `json:"warehouse_id"`
	InTxID      uint `json:"res_tx_id"` // untuk id restock an ketika type reshipping

	RefundPaymentType db_models.RestockPaymentType `json:"refund_payment_type"`
	RefundAmount      float64                      `json:"refund_amount"`
	FundAt            time.Time                    `json:"fund_at"`
	Created           time.Time                    `json:"created"`
}

// Key implements exact_one.ExactHaveKey.
func (i *InvResolution) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "expense_histories",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), i.ID)
}
