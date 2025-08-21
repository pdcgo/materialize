package accounting_core

type AccountKey string

const (
	StockPendingAccount      AccountKey = "stock_pending"
	StockReadyAccount        AccountKey = "stock_ready"
	StockBrokenAmount        AccountKey = "stock_broken"
	StockLostAmount          AccountKey = "stock_lost"
	SupplierPayableAccount   AccountKey = "supplier_payable"
	ShippingPayableAccount   AccountKey = "shipping_payable"
	CashAccount              AccountKey = "cash"
	PayableAccount           AccountKey = "payable"
	ReceivableAccount        AccountKey = "receivable"
	SellingReceivableAccount AccountKey = "selling_receivable"
	ShippingCostAccount      AccountKey = "shipping_cost"
	DebtAccount              AccountKey = "debt"
)
