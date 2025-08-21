package stock_transaction

import (
	"fmt"

	"github.com/pdcgo/materialize/accounting_core"
	"gorm.io/gorm"
)

type PaymentMethod string

const (
	ShopeepayPayment PaymentMethod = "shopeepay"
	BankPayment      PaymentMethod = "bank"
)

type RestockPayload struct {
	TeamID             uint
	WarehouseID        uint
	RestockAmount      float64
	ShippingCostAmount float64
	PaymentMethod      PaymentMethod
}

type AcceptStockPayload struct {
	TeamID         uint
	WarehouseID    uint
	AcceptedAmount float64
	LostAmount     float64
	BrokenAmount   float64
	CodAmount      float64
}

type OrderStockPayload struct {
	TeamID      uint
	WarehouseID uint
	UserID      uint
	ShopID      uint
	Amount      float64
}

type StockTransaction interface {
	BrokenStock() error
	LostStock() error
	OrderStock(payload *OrderStockPayload) error
	Restock(payload *RestockPayload) error
	AcceptStock(payload *AcceptStockPayload) error
}

type stockTransactionImpl struct {
	db *gorm.DB
}

// OrderStock implements StockTransaction.
func (s *stockTransactionImpl) OrderStock(payload *OrderStockPayload) error {
	var err error
	var tran accounting_core.Transaction
	return s.db.Transaction(func(tx *gorm.DB) error {
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

		if err != nil {
			return err
		}

		entry := accounting_core.NewCreateEntry(tx)
		err = entry.
			To(accounting_core.StockReadyAccount, payload.Amount*-1).
			To(accounting_core.SellingReceivableAccount, payload.Amount).
			TransactionID(tran.ID).
			Commit().
			Err()

		if err != nil {
			return err
		}
		return nil
	})

}

// BrokenStock implements StockTransaction.
func (s *stockTransactionImpl) BrokenStock() error {
	panic("unimplemented")
}

// LostStock implements StockTransaction.
func (s *stockTransactionImpl) LostStock() error {
	panic("unimplemented")
}

// AcceptStock implements StockTransaction.
func (s *stockTransactionImpl) AcceptStock(payload *AcceptStockPayload) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		var err error
		var tran accounting_core.Transaction

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
			}).
			Err()

		if err != nil {
			return err
		}

		stockAmount := payload.AcceptedAmount + payload.BrokenAmount + payload.LostAmount

		entry := accounting_core.NewCreateEntry(tx)
		entry.
			To(accounting_core.StockPendingAccount, stockAmount*-1)

		if payload.AcceptedAmount != 0 {
			entry.To(accounting_core.StockReadyAccount, payload.AcceptedAmount)
		}
		if payload.BrokenAmount != 0 {
			entry.To(accounting_core.StockBrokenAmount, payload.BrokenAmount)
		}
		if payload.LostAmount != 0 {
			entry.To(accounting_core.StockLostAmount, payload.LostAmount)
		}

		err = entry.
			TransactionID(tran.ID).
			Commit().
			Err()

		if err != nil {
			return err
		}

		if payload.CodAmount != 0 {
			entry := accounting_core.NewCreateEntry(tx)
			err = entry.
				To(accounting_core.PayableAccount, payload.CodAmount).
				To(accounting_core.ReceivableAccount, payload.CodAmount).
				TransactionID(tran.ID).
				Commit().
				Err()

			if err != nil {
				return err
			}
		}

		return err

	})
}

// Restock implements StockTransaction.
func (s *stockTransactionImpl) Restock(payload *RestockPayload) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		var err error
		var tran accounting_core.Transaction

		err = accounting_core.
			NewTransaction(tx).
			Create(&tran).
			Labels([]*accounting_core.Label{
				{
					Key:   accounting_core.TeamID,
					Value: fmt.Sprintf("%d", payload.TeamID),
				},
				{
					Key:   accounting_core.PaymentMethod,
					Value: string(payload.PaymentMethod),
				},
				{
					Key:   accounting_core.WarehouseID,
					Value: fmt.Sprintf("%d", payload.WarehouseID),
				},
			}).
			Err()

		if err != nil {
			return err
		}

		totalAmount := payload.RestockAmount + payload.ShippingCostAmount
		entry := accounting_core.NewCreateEntry(tx)
		if payload.ShippingCostAmount != 0 {
			entry.
				To(accounting_core.ShippingPayableAccount, payload.ShippingCostAmount)
		}

		err = entry.
			To(accounting_core.SupplierPayableAccount, payload.RestockAmount).
			To(accounting_core.StockPendingAccount, totalAmount).
			TransactionID(tran.ID).
			Commit().
			Err()

		if err != nil {
			return err
		}

		entry = accounting_core.NewCreateEntry(tx)
		err = entry.
			To(accounting_core.CashAccount, totalAmount*-1).
			To(accounting_core.ShippingPayableAccount, payload.ShippingCostAmount*-1).
			To(accounting_core.SupplierPayableAccount, payload.RestockAmount*-1).
			TransactionID(tran.ID).
			Commit().
			Err()

		if err != nil {
			return err
		}

		return nil

	})

}

func NewStockTransaction(db *gorm.DB) StockTransaction {
	return &stockTransactionImpl{
		db: db,
	}
}
