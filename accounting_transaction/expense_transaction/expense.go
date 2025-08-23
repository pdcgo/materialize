package expense_transaction

import "gorm.io/gorm"

type ExpenseTransaction interface {
}

type expenseTransactonImpl struct {
	tx *gorm.DB
}

func NewExpenseTransaction(tx *gorm.DB) ExpenseTransaction {
	return &expenseTransactonImpl{
		tx: tx,
	}
}
