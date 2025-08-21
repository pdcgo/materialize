package accounting_core_test

import (
	"testing"

	"github.com/pdcgo/materialize/accounting_core"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestJournalEntries(t *testing.T) {
	var db gorm.DB

	var migrate moretest.SetupFunc = func(t *testing.T) func() error {
		err := db.AutoMigrate(
			&accounting_core.JournalEntry{},
		)

		assert.Nil(t, err)

		return nil
	}

	var accounts moretest.SetupFunc = func(t *testing.T) func() error {

		err := accounting_core.
			NewCreateAccount(&db).
			Create(
				accounting_core.DebitBalance,
				accounting_core.ASSET,
				1,
				accounting_core.StockPendingAccount,
				"Pending Stock",
			)

		assert.Nil(t, err)

		err = accounting_core.
			NewCreateAccount(&db).
			Create(
				accounting_core.DebitBalance,
				accounting_core.ASSET,
				1,
				accounting_core.CashAccount,
				"Kas",
			)

		assert.Nil(t, err)

		return nil
	}

	moretest.Suite(t, "testing journal entries",
		moretest.SetupListFunc{
			moretest_mock.MockSqliteDatabase(&db),
			migrate,
			accounts,
		},
		func(t *testing.T) {
			err := db.Transaction(func(tx *gorm.DB) error {
				entryCreate := accounting_core.NewCreateEntry(tx)

				return entryCreate.
					To(accounting_core.CashAccount, -1200).
					To(accounting_core.StockPendingAccount, 1200).
					TransactionID(1).
					Commit().
					Err()
			})

			assert.Nil(t, err)

			t.Run("testing getting entry", func(t *testing.T) {
				entries := []*accounting_core.JournalEntry{}

				err = db.
					Model(&accounting_core.JournalEntry{}).
					Where("transaction_id = ?", 1).
					Find(&entries).
					Error
				assert.Nil(t, err)

				assert.Len(t, entries, 2)

				// raw, _ := json.MarshalIndent(entries, "", "  ")
				// t.Error(string(raw))
			})

		},
	)
}
