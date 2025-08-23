package accounting_core

type LabelKey string

const (
	TeamIDLabel        LabelKey = "team_id"
	PaymentMethodLabel LabelKey = "payment_method"
	WarehouseIDLabel   LabelKey = "warehouse_id"
	RefIDLabel         LabelKey = "ref_id"
	SystemIDLabel      LabelKey = "system_id"
	ReceiptLabel       LabelKey = "receipt"
	UserIDLabel        LabelKey = "user_id"
	ShopIDLabel        LabelKey = "shop_id"
)
