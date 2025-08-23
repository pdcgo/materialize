package stock_transaction

import (
	"errors"
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
	Receipt            string
	RefID              string
	RestockAmount      float64
	ShippingCostAmount float64
	PaymentMethod      PaymentMethod
}

type AcceptStockPayload struct {
	TeamID         uint
	WarehouseID    uint
	Receipt        string
	SystemID       uint
	AcceptedAmount float64
	LostAmount     float64
	BrokenAmount   float64
	CodAmount      float64
}

type BrokenStock struct {
	TeamID           uint
	WarehouseID      uint
	PayableAmount    float64
	NotPayableAmount float64
}

type StockTransaction interface {
	BrokenStock(payload *BrokenStock) error
	LostStock() error
	Restock(payload *RestockPayload) error
	AcceptStock(payload *AcceptStockPayload) error
}

type stockTransactionImpl struct {
	db *gorm.DB
}

// BrokenStock implements StockTransaction.
func (s *stockTransactionImpl) BrokenStock(payload *BrokenStock) error {
	if payload.NotPayableAmount == 0 && payload.PayableAmount == 0 {
		return errors.New("payload not have payable or not payable amount")
	}

	return s.db.Transaction(func(tx *gorm.DB) error {
		var err error
		var tran accounting_core.Transaction

		err = accounting_core.
			NewTransaction(tx).
			Create(&tran).
			Labels([]*accounting_core.Label{
				{
					Key:   accounting_core.TeamIDLabel,
					Value: fmt.Sprintf("%d", payload.TeamID),
				},
				{
					Key:   accounting_core.WarehouseIDLabel,
					Value: fmt.Sprintf("%d", payload.WarehouseID),
				},
			}).
			Err()

		if err != nil {
			return err
		}

		if payload.PayableAmount != 0 {
			entry := accounting_core.NewCreateEntry(tx, payload.TeamID)
			err = entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockReadyAccount,
					TeamID: payload.TeamID,
				}, payload.PayableAmount).
				To(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.PayableAccount,
					TeamID: payload.WarehouseID,
				}, payload.PayableAmount).
				Transaction(&tran).
				Commit().
				Err()

			if err != nil {
				return err
			}

			// warehouse entry
			entry = accounting_core.NewCreateEntry(tx, payload.WarehouseID)
			err = entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockReadyAccount,
					TeamID: payload.WarehouseID,
				}, payload.PayableAmount).
				To(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.PayableAccount,
					TeamID: payload.TeamID,
				}, payload.PayableAmount).
				Transaction(&tran).
				Commit().
				Err()

			if err != nil {
				return err
			}

		}

		if payload.NotPayableAmount != 0 {
			entry := accounting_core.NewCreateEntry(tx, payload.TeamID)
			err = entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockReadyAccount,
					TeamID: payload.TeamID,
				}, payload.NotPayableAmount).
				To(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockBrokenAccount,
					TeamID: payload.WarehouseID,
				}, payload.NotPayableAmount).
				Transaction(&tran).
				Commit().
				Err()

			if err != nil {
				return err
			}

			// warehouse entry
			entry = accounting_core.NewCreateEntry(tx, payload.WarehouseID)
			err = entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockReadyAccount,
					TeamID: payload.WarehouseID,
				}, payload.NotPayableAmount).
				To(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.StockBrokenAccount,
					TeamID: payload.WarehouseID,
				}, payload.NotPayableAmount).
				Transaction(&tran).
				Commit().
				Err()

			if err != nil {
				return err
			}
		}

		return nil
	})
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
					Key:   accounting_core.TeamIDLabel,
					Value: fmt.Sprintf("%d", payload.TeamID),
				},
				{
					Key:   accounting_core.WarehouseIDLabel,
					Value: fmt.Sprintf("%d", payload.WarehouseID),
				},
				{
					Key:   accounting_core.ReceiptLabel,
					Value: fmt.Sprintf("%s", payload.Receipt),
				},
				{
					Key:   accounting_core.SystemIDLabel,
					Value: fmt.Sprintf("%d", payload.SystemID),
				},
			}).
			Err()

		if err != nil {
			return err
		}

		entry := accounting_core.NewCreateEntry(tx, payload.TeamID)
		entry.
			From(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.StockPendingAccount,
				TeamID: payload.TeamID,
			}, payload.AcceptedAmount+payload.BrokenAmount+payload.LostAmount).
			To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.StockReadyAccount,
				TeamID: payload.TeamID,
			}, payload.AcceptedAmount+payload.CodAmount).
			To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.PayableAccount,
				TeamID: payload.WarehouseID,
			}, payload.CodAmount)

		if payload.BrokenAmount != 0 || payload.LostAmount != 0 {
			entry.To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.SuplierReceivableAccount,
				TeamID: payload.TeamID,
			}, payload.BrokenAmount+payload.LostAmount)
		}

		err = entry.
			TransactionID(tran.ID).
			Commit().
			Err()

		if err != nil {
			return err
		}

		if payload.CodAmount != 0 {
			entry := accounting_core.NewCreateEntry(tx, payload.WarehouseID)
			err = entry.
				From(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.CashAccount,
					TeamID: payload.WarehouseID,
				}, payload.CodAmount).
				To(&accounting_core.EntryAccountPayload{
					Key:    accounting_core.ReceivableAccount,
					TeamID: payload.TeamID,
				}, payload.CodAmount).
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
					Key:   accounting_core.TeamIDLabel,
					Value: fmt.Sprintf("%d", payload.TeamID),
				},
				{
					Key:   accounting_core.PaymentMethodLabel,
					Value: string(payload.PaymentMethod),
				},
				{
					Key:   accounting_core.WarehouseIDLabel,
					Value: fmt.Sprintf("%d", payload.WarehouseID),
				},
				{
					Key: accounting_core.ReceiptLabel,
				},
			}).
			Err()

		if err != nil {
			return err
		}

		totalAmount := payload.RestockAmount + payload.ShippingCostAmount
		entry := accounting_core.NewCreateEntry(tx, payload.TeamID)
		err = entry.
			From(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.CashAccount,
				TeamID: payload.TeamID,
			}, totalAmount).
			To(&accounting_core.EntryAccountPayload{
				Key:    accounting_core.StockPendingAccount,
				TeamID: payload.TeamID,
			}, totalAmount).
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
