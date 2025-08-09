package models

import (
	"fmt"
	"time"

	"github.com/pdcgo/materialize/stat_replica"
)

type BalanceAccountHistory struct {
	ID        uint `json:"id"`
	TeamID    uint `json:"team_id"`
	AccountID uint `json:"account_id"`

	Amount    float64   `json:"amount"`
	At        time.Time `json:"at"`
	CreatedAt time.Time `json:"created_at"`
}

// Key implements exact_one.ExactHaveKey.
func (b *BalanceAccountHistory) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "balance_account_histories",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), b.ID)
}

type ExpenseAccount struct {
	ID            uint      `json:"id" gorm:"primarykey"`
	TeamID        uint      `json:"team_id"`
	AccountTypeID uint      `json:"account_type_id" gorm:"index:account_number_unique,unique"`
	NumberID      string    `json:"number_id" gorm:"index:account_number_unique,unique"`
	Name          string    `json:"name"`
	Disabled      bool      `json:"disabled"`
	CreatedAt     time.Time `json:"created_at"`
}

// Key implements exact_one.ExactHaveKey.
func (e *ExpenseAccount) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "expense_accounts",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), e.ID)
}

type ExpenseHistory struct {
	ID          uint `json:"id" gorm:"primarykey"`
	TeamID      uint `json:"team_id"`
	CategoryID  uint `json:"category_id"`
	CreatedByID uint `json:"created_by_id"`

	Amount    float64   `json:"amount"`
	At        time.Time `json:"at"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
}

// Key implements exact_one.ExactHaveKey.
func (e *ExpenseHistory) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "inv_resolutions",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), e.ID)
}

type AdsExpenseHistory struct {
	ID            uint `json:"id" gorm:"primarykey"`
	TeamID        uint `json:"team_id"`
	CreatedByID   uint `json:"created_by_id"`
	MarketplaceID uint `json:"marketplace_id"`

	Amount    float64   `json:"amount"`
	At        time.Time `json:"at"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
}

func (e *AdsExpenseHistory) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "ads_expense_histories",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), e.ID)
}
