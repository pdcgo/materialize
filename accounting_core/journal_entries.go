package accounting_core

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

var ErrEmptyEntry = errors.New("entry empty")

type ErrEntryInvalid struct {
	Debit  float64 `json:"debit"`
	Credit float64 `json:"credit"`
}

// Error implements error.
func (e *ErrEntryInvalid) Error() string {
	raw, _ := json.Marshal(e)
	return "journal entry invalid" + string(raw)
}

// var ErrEntryInvalid = errors.New("journal entry invalid")

type CreateEntry interface {
	Commit() CreateEntry
	Desc(desc string) CreateEntry
	TransactionID(txID uint) CreateEntry
	// From(accID uint, amount float64) CreateEntry
	To(key AccountKey, amount float64) CreateEntry
	Err() error
}

type createEntryImpl struct {
	tx      *gorm.DB
	entries map[uint]*JournalEntry
	err     error
}

// Commit implements CreateEntry.
func (c *createEntryImpl) Commit() CreateEntry {
	if c.isEntryEmpty() {
		return c.setErr(ErrEmptyEntry)
	}
	var entries []*JournalEntry

	var debit, credit float64

	for _, entry := range c.entries {
		entry.EntryTime = time.Now()
		debit += entry.Debit
		credit += entry.Credit

		entries = append(entries, entry)
	}

	// checking debit and credit balance
	if debit != credit {
		return c.setErr(&ErrEntryInvalid{
			Debit:  debit,
			Credit: credit,
		})
	}

	err := c.tx.Save(&entries).Error
	if err != nil {
		return c.setErr(err)
	}

	return c
}

// Desc implements CreateEntry.
func (c *createEntryImpl) Desc(desc string) CreateEntry {
	if c.isEntryEmpty() {
		return c.setErr(ErrEmptyEntry)
	}

	for _, entry := range c.entries {
		entry.Desc = desc
	}
	return c
}

// Err implements CreateEntry.
func (c *createEntryImpl) Err() error {
	return c.err
}

// To implements CreateEntry.
func (c *createEntryImpl) To(key AccountKey, amount float64) CreateEntry {
	acc, err := c.getAccount(key)
	if err != nil {
		return c.setErr(err)
	}

	entry := &JournalEntry{
		AccountID: acc.ID,
	}

	err = acc.SetAmountEntry(amount, entry)
	if err != nil {
		return c.setErr(err)
	}
	c.entries[acc.ID] = entry

	return c
}

// TransactionID implements CreateEntry.
func (c *createEntryImpl) TransactionID(txID uint) CreateEntry {
	if c.isEntryEmpty() {
		return c.setErr(ErrEmptyEntry)
	}

	for _, entry := range c.entries {
		entry.TransactionID = txID
	}
	return c
}

func (c *createEntryImpl) getAccount(key AccountKey) (*Account, error) {
	var acc Account
	var err error

	err = c.tx.Model(&Account{}).Where("key = ?", key).Find(&acc).Error
	if err != nil {
		return &acc, err
	}

	if acc.ID == 0 {
		return &acc, fmt.Errorf("account not found %s", key)
	}

	return &acc, nil
}
func (c *createEntryImpl) isEntryEmpty() bool {
	return len(c.entries) == 0
}

func (c *createEntryImpl) setErr(err error) *createEntryImpl {
	if c.err != nil {
		return c
	}

	if err != nil {
		c.err = err
	}

	return c
}

func NewCreateEntry(tx *gorm.DB) CreateEntry {
	return &createEntryImpl{
		tx:      tx,
		entries: map[uint]*JournalEntry{},
	}
}

type journalEntriesImpl struct {
	tx *gorm.DB
}
