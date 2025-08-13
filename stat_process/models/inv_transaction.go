package models

import (
	"fmt"
	"time"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
)

type InvTransaction struct {
	ID          uint `json:"id" gorm:"primarykey"`
	TeamID      uint `json:"team_id"`
	WarehouseID uint `json:"warehouse_id"`
	CreateByID  uint `json:"create_by_id"` // field bakalan deprecated and removed
	// VerifyByID  *uint `json:"verify_by_id"`
	// ShippingID  *uint `json:"shipping_id"`

	ExternOrdID string `json:"extern_ord_id" gorm:"index"`

	IsBroken        bool `json:"is_broken"`
	IsBrokenPartial bool `json:"is_broken_partial"`

	Receipt     string `json:"receipt" gorm:"index"`
	ReceiptFile string `json:"receipt_file"`

	Type      db_models.InvTxType   `json:"type"`
	Status    db_models.InvTxStatus `json:"status"`
	IsShipped bool                  `json:"is_shipped"`
	Deleted   bool                  `json:"deleted" gorm:"index"`

	Arrived time.Time `json:"arrived"`
	SendAt  time.Time `json:"send_at"`
	Created time.Time `json:"created" gorm:"index"`

	Total float64 `json:"total"`
}

// Key implements exact_one.ExactHaveKey.
func (i *InvTransaction) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "inv_transactions",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), i.ID)
}
