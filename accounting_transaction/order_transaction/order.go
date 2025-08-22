package order_transaction

import (
	"fmt"

	"github.com/pdcgo/materialize/accounting_core"
	"gorm.io/gorm"
)

type CrossProductAmount struct {
	TeamID uint
	Amount float64
}

type CrossProductAmountList []*CrossProductAmount

func (lst CrossProductAmountList) Total() float64 {
	var total float64
	for _, item := range lst {
		total += item.Amount
	}
	return total
}

type CreateOrderPayload struct {
	TeamID             uint
	WarehouseID        uint
	UserID             uint
	ShopID             uint
	OwnProductAmount   float64
	CrossProductAmount CrossProductAmountList
}

type OrderTransaction interface {
	CreateOrder(payload *CreateOrderPayload) error
	WithdrawalOrder() error
	AdjustmentOrder() error
	ReturnOrder() error
	ProblemOrder() error
}

type orderTransactionImpl struct {
	tx *gorm.DB
}

// AdjustmentOrder implements OrderTransaction.
func (o *orderTransactionImpl) AdjustmentOrder() error {
	panic("unimplemented")
}

// CreateOrder implements OrderTransaction.
func (o *orderTransactionImpl) CreateOrder(payload *CreateOrderPayload) error {
	var tran accounting_core.Transaction
	var err error
	err = o.tx.Transaction(func(tx *gorm.DB) error {
		err = accounting_core.
			NewTransaction(tx).
			Create(&tran).
			Labels([]*accounting_core.Label{
				{
					Key:   accounting_core.TeamID,
					Value: fmt.Sprintf("%d", payload.TeamID),
				},
				{
					Key:   accounting_core.WarehouseID,
					Value: fmt.Sprintf("%d", payload.WarehouseID),
				},
				{
					Key:   accounting_core.UserID,
					Value: fmt.Sprintf("%d", payload.UserID),
				},
				{
					Key:   accounting_core.ShopID,
					Value: fmt.Sprintf("%d", payload.ShopID),
				},
			}).
			Err()

		entry := accounting_core.NewCreateEntry(tx, payload.TeamID)
		err = entry.
			From(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.StockReadyAccount,
				TeamID: payload.TeamID,
			}, payload.OwnProductAmount).
			To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.SellingReceivableAccount,
				TeamID: payload.TeamID,
			}, payload.OwnProductAmount).
			TransactionID(tran.ID).
			Err()

		if err != nil {
			return err
		}

		if len(payload.CrossProductAmount) != 0 {
			crossTotal := payload.CrossProductAmount.Total()

			entry = accounting_core.NewCreateEntry(tx, payload.TeamID)

			entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockCrossAccount,
					TeamID: payload.TeamID,
				}, crossTotal)

			for _, cros := range payload.CrossProductAmount {
				entry.
					To(&accounting_core.EntryAccountPayload{
						Key:    accounting_core.PayableAccount,
						TeamID: cros.TeamID,
					}, cros.Amount)

				entry.
					From(&accounting_core.EntryAccountPayload{
						Key:    accounting_core.StockReadyAccount,
						TeamID: cros.TeamID,
					}, cros.Amount).
					To(&accounting_core.EntryAccountPayload{
						Key:    accounting_core.StockCrossReceivableAccount,
						TeamID: cros.TeamID,
					}, cros.Amount)
			}

			entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockCrossAccount,
					TeamID: payload.TeamID,
				}, crossTotal).
				To(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.SellingReceivableAccount,
					TeamID: payload.TeamID,
				}, crossTotal).
				TransactionID(tran.ID).
				Err()

			if err != nil {
				return err
			}

		}

		return nil
	})

	return err
}

// ProblemOrder implements OrderTransaction.
func (o *orderTransactionImpl) ProblemOrder() error {
	panic("unimplemented")
}

// ReturnOrder implements OrderTransaction.
func (o *orderTransactionImpl) ReturnOrder() error {
	panic("unimplemented")
}

// WithdrawalOrder implements OrderTransaction.
func (o *orderTransactionImpl) WithdrawalOrder() error {
	panic("unimplemented")
}

func NewOrderTransaction(tx *gorm.DB) OrderTransaction {
	return &orderTransactionImpl{
		tx: tx,
	}
}
