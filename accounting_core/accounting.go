package accounting_core

type AccountingService interface {
	CreateEntry() CreateEntry
	CreateAccount() CreateAccount
	CreateTransaction() CreateTransaction
}
