package accounting_core

type LabelKey string

const (
	TeamID        LabelKey = "team_id"
	PaymentMethod LabelKey = "payment_method"
	WarehouseID   LabelKey = "warehouse_id"
	UserID        LabelKey = "user_id"
	ShopID        LabelKey = "shop_id"
)
