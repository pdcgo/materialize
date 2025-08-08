package models

import (
	"fmt"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
)

type RestockCost struct {
	ID               uint                         `json:"id" gorm:"primarykey"`
	InvTransactionID uint                         `json:"inv_transaction_id"`
	PaymentType      db_models.RestockPaymentType `json:"payment_type"`
	ShippingFee      float64                      `json:"shipping_fee"`
	CodFee           float64                      `json:"cod_fee"`
	OtherFee         float64                      `json:"other_fee"`
	PerPieceFee      float64                      `json:"per_piece_fee"`
}

// Key implements exact_one.ExactHaveKey.
func (r *RestockCost) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "restock_costs",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), r.ID)
}
