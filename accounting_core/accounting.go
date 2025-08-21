package accounting_core

type CreateAccount interface {
	Create(tipe BalanceType, coa CoaCode, domainID uint, key AccountKey, name string) error
}

type AccountingService interface {
	CreateEntry() CreateEntry
	CreateAccount() CreateAccount
	CreateTransaction() CreateTransaction
}
