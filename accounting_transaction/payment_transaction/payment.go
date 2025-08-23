package payment_transaction

import (
	"github.com/pdcgo/materialize/accounting_core"
	"gorm.io/gorm"
)

type PaymentPayload struct {
	ToTeamID   uint    `json:"to_team_id"`
	FromTeamID uint    `json:"from_team_id"`
	Desc       string  `json:"desc"`
	Amount     float64 `json:"amount"`
}

type PaymentTransaction interface {
	Payment(payment *PaymentPayload) error
}

type paymentPaymentTransactionImpl struct {
	tx *gorm.DB
}

// Payment implements PaymentTransaction.
func (p *paymentPaymentTransactionImpl) Payment(payment *PaymentPayload) error {
	return p.tx.Transaction(func(tx *gorm.DB) error {
		var err error
		var tran accounting_core.Transaction

		err = accounting_core.
			NewTransaction(tx).
			Create(&tran).
			Err()

		if err != nil {
			return err
		}

		entry := accounting_core.NewCreateEntry(tx, payment.FromTeamID)
		err = entry.
			To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.CashAccount,
				TeamID: payment.FromTeamID,
			}, payment.Amount).
			From(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.PayableAccount,
				TeamID: payment.ToTeamID,
			}, payment.Amount).
			Transaction(&tran).
			Commit().
			Err()

		if err != nil {
			return err
		}

		entry = accounting_core.NewCreateEntry(tx, payment.ToTeamID)
		err = entry.
			To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.CashAccount,
				TeamID: payment.ToTeamID,
			}, payment.Amount).
			From(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.ReceivableAccount,
				TeamID: payment.FromTeamID,
			}, payment.Amount).
			Transaction(&tran).
			Commit().
			Err()

		if err != nil {
			return err
		}

		return nil
	})
}

func NewPaymentTransaction(tx *gorm.DB) PaymentTransaction {
	return &paymentPaymentTransactionImpl{
		tx: tx,
	}
}
