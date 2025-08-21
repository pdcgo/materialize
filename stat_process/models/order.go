package models

import (
	"fmt"
	"time"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/interfaces/identity_iface"
)

type Order struct {
	ID                  uint `json:"id" gorm:"primarykey"`
	TeamID              uint `json:"team_id"`
	CreatedByID         uint `json:"created_by_id"`
	InvertoryTxID       uint `json:"invertory_tx_id"`
	InvertoryReturnTxID uint `json:"invertory_ret_tx_id"`
	DoubleOrder         bool `json:"double_order"`

	OrderRefID   string                `json:"order_ref_id" gorm:"index"`
	OrderFrom    db_models.OrderMpType `json:"order_from"`
	OrderMpTotal int                   `json:"order_mp_total"`

	// bagian tipe2 order
	ProductSourceType db_models.ProductSourceType `json:"product_source"`
	ParentPartialID   uint                        `json:"parent_partial_id"`
	IsPartial         bool                        `json:"is_partial"`
	IsOrderFake       bool                        `json:"is_order_fake"`

	// perkara wd fund
	WdTotal float64   `json:"wd_total"`
	WdTime  time.Time `json:"wd_time"`
	WdSet   bool      `json:"wd_set"`

	WdFundAt time.Time `json:"wd_fund_at"`
	WdFund   bool      `json:"wd_fund"`

	Adjustment float64 `json:"adjustment"`

	OrderTime time.Time `json:"order_time" gorm:"index"`
	OrderMpID uint      `json:"order_mp_id"`

	Receipt           string `json:"receipt"`
	ReceiptFile       string `json:"receipt_file"`
	ReceiptReturn     string `json:"receipt_return"`
	ReceiptReturnFile string `json:"receipt_return_file"`

	Status       db_models.OrdStatus `json:"status"`
	WarehouseFee float64             `json:"warehouse_fee"`
	ShipmentFee  float64             `json:"shipping_fee"`
	ItemCount    int                 `json:"item_count"`
	Total        float64             `json:"total"`
	CreatedAt    time.Time           `json:"created_at" gorm:"index"`
	// AdditionalCosts string              `json:"additional_costs"`
	// AdditionalCosts datatypes.JSONSlice[*db_models.OrderAdditionalCost] `json:"additional_costs"`
}

// Key implements exact_one.ExactHaveKey.
func (o *Order) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "orders",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), o.ID)
}

type OrderAdjustment struct {
	ID      uint `json:"id" gorm:"primarykey"`
	OrderID uint `json:"order_id"`
	MpID    uint `json:"mp_id"`

	At     time.Time                `json:"at" gorm:"index"`
	FundAt time.Time                `json:"fund_at" gorm:"index"`
	Type   db_models.AdjustmentType `json:"type"`
	Amount float64                  `json:"amount"`
	Desc   string                   `json:"desc"`
}

// Key implements exact_one.ExactHaveKey.
func (o *OrderAdjustment) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "order_adjustments",
		Schema: "public",
	}

	return fmt.Sprintf("%s%d/%s-%d", meta.PrefixKey(), o.OrderID, o.Type, o.FundAt.Unix())
}

type OrderTimestamp struct {
	ID          uint                     `json:"id" gorm:"primarykey"`
	OrderID     uint                     `json:"order_id"`
	UserID      uint                     `json:"user_id"`
	From        identity_iface.AgentType `json:"from"`
	OrderStatus db_models.OrdStatus      `json:"order_status"`
	Timestamp   time.Time                `json:"timestamp" gorm:"index"`
}

type InvOrderData struct {
	InvID        uint    `json:"inv_id"`
	OrderID      uint    `json:"order_id"`
	TeamID       uint    `json:"team_id"`
	MpID         uint    `json:"mp_id"`
	WarehouseFee float64 `json:"warehouse_fee"`
	MpTotal      float64 `json:"mp_total"`
}

func (o *InvOrderData) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "inv_order",
		Schema: "cache",
	}

	return fmt.Sprintf("%s%d", meta.PrefixKey(), o.InvID)
}
