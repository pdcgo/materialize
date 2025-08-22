package accounting_core

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"
)

type CoaCode int

const (
	ASSET     CoaCode = 10
	LIABILITY CoaCode = 20
	EQUITY    CoaCode = 30
	REVENUE   CoaCode = 40
	EXPENSE   CoaCode = 50
)

type BalanceType string

const (
	CreditBalance BalanceType = "c"
	DebitBalance  BalanceType = "d"
)

type JournalEntry struct {
	ID            uint      `json:"id" gorm:"primarykey"`
	AccountID     uint      `json:"account_id"`
	TeamID        uint      `json:"team_id"`
	TransactionID uint      `json:"transaction_id"`
	EntryTime     time.Time `json:"entry_time"`
	Debit         float64   `json:"debit"`
	Credit        float64   `json:"credit"`
	Desc          string    `json:"desc"`

	Account *Account `json:"account"`
}

type Account struct {
	ID          uint        `json:"id" gorm:"primarykey"`
	TeamID      uint        `json:"team_id"`
	Coa         CoaCode     `json:"coa"`
	BalanceType BalanceType `json:"account_type"`
	Key         AccountKey  `json:"key" gorm:"index:domain_key,unique"`
	Name        string      `json:"name"`

	Created time.Time `json:"created"`
}

func (ac *Account) SetAmountEntry(amount float64, entry *JournalEntry) error {
	if amount == 0 {
		return errors.New("amount entry set is zero")
	}

	amountAbs := math.Abs(amount)

	switch ac.BalanceType {
	case CreditBalance:
		if amount > 0 {
			entry.Credit = amountAbs
		}
		if amount < 0 {
			entry.Debit = amountAbs
		}
	case DebitBalance:
		if amount > 0 {
			entry.Debit = amountAbs
		}
		if amount < 0 {
			entry.Credit = amountAbs
		}
	default:
		return fmt.Errorf("account type invalid %s", ac.BalanceType)
	}

	return nil
}

type TransactionType string

type Transaction struct {
	ID      uint            `json:"id" gorm:"primarykey"`
	RefID   string          `json:"ref_id"`
	Type    TransactionType `json:"type"`
	Desc    string          `json:"desc"`
	Created time.Time       `json:"created"`
}

type TransactionLabel struct {
	ID            uint   `json:"id" gorm:"primarykey"`
	TransactionID uint   `json:"transaction_id" gorm:"index:tx_labels,unique"`
	LabelID       string `json:"label_id" gorm:"index:tx_labels,unique"`

	Label       *Label       `json:"-"`
	Transaction *Transaction `json:"-"`
}

type Label struct {
	ID    string   `json:"id" gorm:"primarykey"`
	Key   LabelKey `json:"key" gorm:"index:keyval,unique"`
	Value string   `json:"value" gorm:"index:keyval,unique"`
}

func (l *Label) Hash() string {
	sum := md5.Sum([]byte(string(l.Key) + l.Value))
	hashid := hex.EncodeToString(sum[:])
	l.ID = hashid
	return hashid
}
