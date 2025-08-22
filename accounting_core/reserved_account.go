package accounting_core

type AccountKey string

const (
	CashAccount                 AccountKey = "cash"
	SuplierCashAccount          AccountKey = "supplier_cash"
	StockPendingAccount         AccountKey = "stock_pending"
	StockReadyAccount           AccountKey = "stock_ready"
	StockBrokenAccount          AccountKey = "stock_broken"
	StockLostAccount            AccountKey = "stock_lost"
	StockCrossAccount           AccountKey = "stock_cross"
	SupplierPayableAccount      AccountKey = "supplier_payable"
	ShippingPayableAccount      AccountKey = "shipping_payable"
	PayableAccount              AccountKey = "payable"
	ReceivableAccount           AccountKey = "receivable"
	StockCrossReceivableAccount AccountKey = "stock_cross_receivable"
	StockCrossPayableAccount    AccountKey = "stock_cross_payable"
	SellingReceivableAccount    AccountKey = "selling_receivable"
	DebtAccount                 AccountKey = "debt"
)

// var ChartOfAccounts = []Account{
// 	// Assets
// 	{Code: "1000", Name: "Assets", Type: Asset},
// 	{Code: "1100", Name: "Current Assets", Type: Asset, Parent: "1000"},
// 	{Code: "1110", Name: "Cash", Type: Asset, Parent: "1100"},
// 	{Code: "1120", Name: "Bank", Type: Asset, Parent: "1100"},
// 	{Code: "1130", Name: "Accounts Receivable", Type: Asset, Parent: "1100"},
// 	{Code: "1140", Name: "Inventory", Type: Asset, Parent: "1100"},
// 	{Code: "1150", Name: "Prepaid Expenses", Type: Asset, Parent: "1100"},
// 	{Code: "1200", Name: "Fixed Assets", Type: Asset, Parent: "1000"},
// 	{Code: "1210", Name: "Equipment", Type: Asset, Parent: "1200"},
// 	{Code: "1220", Name: "Buildings", Type: Asset, Parent: "1200"},
// 	{Code: "1230", Name: "Vehicles", Type: Asset, Parent: "1200"},
// 	{Code: "1240", Name: "Accumulated Depreciation", Type: Asset, Parent: "1200"},

// 	// Liabilities
// 	{Code: "2000", Name: "Liabilities", Type: Liability},
// 	{Code: "2100", Name: "Current Liabilities", Type: Liability, Parent: "2000"},
// 	{Code: "2110", Name: "Accounts Payable", Type: Liability, Parent: "2100"},
// 	{Code: "2120", Name: "Salaries Payable", Type: Liability, Parent: "2100"},
// 	{Code: "2130", Name: "Taxes Payable", Type: Liability, Parent: "2100"},
// 	{Code: "2200", Name: "Long-Term Liabilities", Type: Liability, Parent: "2000"},
// 	{Code: "2210", Name: "Bank Loan", Type: Liability, Parent: "2200"},
// 	{Code: "2220", Name: "Bonds Payable", Type: Liability, Parent: "2200"},

// 	// Equity
// 	{Code: "3000", Name: "Equity", Type: Equity},
// 	{Code: "3110", Name: "Owner’s Capital", Type: Equity, Parent: "3000"},
// 	{Code: "3120", Name: "Retained Earnings", Type: Equity, Parent: "3000"},
// 	{Code: "3130", Name: "Dividends / Drawings", Type: Equity, Parent: "3000"},

// 	// Revenue
// 	{Code: "4000", Name: "Revenue", Type: Revenue},
// 	{Code: "4110", Name: "Sales Revenue", Type: Revenue, Parent: "4000"},
// 	{Code: "4120", Name: "Service Revenue", Type: Revenue, Parent: "4000"},
// 	{Code: "4130", Name: "Interest Income", Type: Revenue, Parent: "4000"},

// 	// Expenses
// 	{Code: "5000", Name: "Expenses", Type: Expense},
// 	{Code: "5100", Name: "Operating Expenses", Type: Expense, Parent: "5000"},
// 	{Code: "5110", Name: "Cost of Goods Sold", Type: Expense, Parent: "5100"},
// 	{Code: "5120", Name: "Rent Expense", Type: Expense, Parent: "5100"},
// 	{Code: "5130", Name: "Salaries Expense", Type: Expense, Parent: "5100"},
// 	{Code: "5140", Name: "Utilities Expense", Type: Expense, Parent: "5100"},
// 	{Code: "5150", Name: "Advertising Expense", Type: Expense, Parent: "5100"},
// 	{Code: "5200", Name: "Administrative Expenses", Type: Expense, Parent: "5000"},
// 	{Code: "5210", Name: "Office Supplies", Type: Expense, Parent: "5200"},
// 	{Code: "5220", Name: "Depreciation Expense", Type: Expense, Parent: "5200"},
// 	{Code: "5230", Name: "Insurance Expense", Type: Expense, Parent: "5200"},
// 	{Code: "5300", Name: "Financial Expenses", Type: Expense, Parent: "5000"},
// 	{Code: "5310", Name: "Bank Charges", Type: Expense, Parent: "5300"},
// 	{Code: "5320", Name: "Interest Expense", Type: Expense, Parent: "5300"},
// }
