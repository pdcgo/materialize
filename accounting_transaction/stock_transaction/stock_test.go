package stock_transaction_test

import (
	"testing"

	"github.com/pdcgo/materialize/accounting_core"
	"github.com/pdcgo/materialize/accounting_transaction/stock_transaction"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestStockOps(t *testing.T) {
	var db gorm.DB

	var migrate moretest.SetupFunc = func(t *testing.T) func() error {
		err := db.AutoMigrate(
			&accounting_core.Transaction{},
			&accounting_core.JournalEntry{},
			&accounting_core.Label{},
			&accounting_core.TransactionLabel{},
		)

		assert.Nil(t, err)
		return nil
	}

	var seed moretest.SetupFunc = func(t *testing.T) func() error {
		accounts := []*accounting_core.Account{
			{
				ID:          1,
				Coa:         accounting_core.ASSET,
				BalanceType: accounting_core.DebitBalance,
				Key:         accounting_core.StockPendingAccount,
				TeamID:      1,
			},
			{
				ID:          2,
				Coa:         accounting_core.LIABILITY,
				BalanceType: accounting_core.CreditBalance,
				Key:         accounting_core.ShippingPayableAccount,
				TeamID:      1,
			},
			{
				ID:          3,
				Coa:         accounting_core.ASSET,
				BalanceType: accounting_core.DebitBalance,
				Key:         accounting_core.CashAccount,
				TeamID:      1,
			},
			{
				ID:          4,
				Coa:         accounting_core.LIABILITY,
				BalanceType: accounting_core.CreditBalance,
				Key:         accounting_core.ShippingPayableAccount,
				TeamID:      1,
			},
			{
				ID:          5,
				Coa:         accounting_core.LIABILITY,
				BalanceType: accounting_core.CreditBalance,
				Key:         accounting_core.SupplierPayableAccount,
				TeamID:      1,
			},
		}
		err := db.Save(&accounts).Error
		assert.Nil(t, err)

		return nil
	}

	moretest.Suite(t, "testing stock operation",
		moretest.SetupListFunc{
			moretest_mock.MockSqliteDatabase(&db),
			migrate,
			seed,
		},
		func(t *testing.T) {

			stockOps := stock_transaction.NewStockTransaction(&db)

			t.Run("testing restock", func(t *testing.T) {
				err := stockOps.Restock(&stock_transaction.RestockPayload{
					TeamID:             1,
					WarehouseID:        1,
					RestockAmount:      12000,
					ShippingCostAmount: 5000,
					PaymentMethod:      stock_transaction.BankPayment,
				})

				assert.Nil(t, err)
				t.Run("testing entry", func(t *testing.T) {
					entries := []*accounting_core.JournalEntry{}
					err = db.Model(&accounting_core.JournalEntry{}).Where("transaction_id = ?", 1).Find(&entries).Error
					assert.Nil(t, err)

					for _, entry := range entries {
						switch entry.AccountID {
						case 3:
							assert.Equal(t, 17000.00, entry.Credit) // cash
						case 2:
							assert.Equal(t, 5000.0, entry.Credit) // shipping cost
						case 1:
							assert.Equal(t, 17000.0, entry.Debit) // stock
						}
					}

					// raw, _ := json.MarshalIndent(entries, "", "  ")
					// log.Println(string(raw))
				})
			})

			moretest.Suite(t, "testing accept stock",
				moretest.SetupListFunc{
					func(t *testing.T) func() error { // initating account
						accounts := []*accounting_core.Account{
							{
								ID:          4,
								Coa:         accounting_core.ASSET,
								BalanceType: accounting_core.DebitBalance,
								Key:         accounting_core.StockReadyAccount,
							},
							{
								ID:          5,
								Coa:         accounting_core.ASSET,
								BalanceType: accounting_core.DebitBalance,
								Key:         accounting_core.StockBrokenAccount,
							},
							{
								ID:          6,
								Coa:         accounting_core.ASSET,
								BalanceType: accounting_core.DebitBalance,
								Key:         accounting_core.StockLostAccount,
							},
							{
								ID:          7,
								Coa:         accounting_core.ASSET,
								BalanceType: accounting_core.DebitBalance,
								Key:         accounting_core.PayableAccount,
							},
							{
								ID:          8,
								Coa:         accounting_core.ASSET,
								BalanceType: accounting_core.DebitBalance,
								Key:         accounting_core.ReceivableAccount,
							},
						}
						err := db.Save(&accounts).Error
						assert.Nil(t, err)
						return nil
					},
				},
				func(t *testing.T) {
					err := stockOps.AcceptStock(&stock_transaction.AcceptStockPayload{
						TeamID:         1,
						WarehouseID:    1,
						AcceptedAmount: 12000,
						LostAmount:     3000,
						BrokenAmount:   1000,
						CodAmount:      2000,
					})
					assert.Nil(t, err)
				},
			)

		},
	)

}
