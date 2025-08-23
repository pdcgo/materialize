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
	Debit  float64            `json:"debit"`
	Credit float64            `json:"credit"`
	List   JournalEntriesList `json:"list"`
}

// Error implements error.
func (e *ErrEntryInvalid) Error() string {
	raw, _ := json.Marshal(e)
	return "journal entry invalid" + string(raw)
}

type EntryAccountPayload struct {
	Key    AccountKey
	TeamID uint
}

type CreateEntry interface {
	Commit() CreateEntry
	Desc(desc string) CreateEntry
	TransactionID(txID uint) CreateEntry
	Transaction(tx *Transaction) CreateEntry
	From(account *EntryAccountPayload, amount float64) CreateEntry
	To(account *EntryAccountPayload, amount float64) CreateEntry
	Err() error
}

type createEntryImpl struct {
	tx      *gorm.DB
	teamID  uint
	entries map[uint]*JournalEntry
	err     error
}

// Transaction implements CreateEntry.
func (c *createEntryImpl) Transaction(tx *Transaction) CreateEntry {
	if c.isEntryEmpty() {
		return c.setErr(ErrEmptyEntry)
	}

	for _, entry := range c.entries {
		entry.Desc = tx.Desc
		entry.TransactionID = tx.ID
	}
	return c
}

// From implements CreateEntry.
func (c *createEntryImpl) From(account *EntryAccountPayload, amount float64) CreateEntry {
	return c.To(account, amount*-1)
}

// Commit implements CreateEntry.
func (c *createEntryImpl) Commit() CreateEntry {
	if c.isEntryEmpty() {
		return c.setErr(ErrEmptyEntry)
	}
	var entries JournalEntriesList

	var debit, credit float64

	for _, entry := range c.entries {
		entry.EntryTime = time.Now()
		entry.TeamID = c.teamID

		debit += entry.Debit
		credit += entry.Credit

		entries = append(entries, entry)
	}

	// entries.PrintJournalEntries(c.tx)

	// checking debit and credit balance
	if debit != credit {

		return c.setErr(&ErrEntryInvalid{
			Debit:  debit,
			Credit: credit,
			List:   entries,
		})
	}

	err := c.tx.Save(&entries).Error
	if err != nil {
		return c.setErr(err)
	}

	return c.
		updateBalance(entries)
}

func (c *createEntryImpl) updateBalance(entries JournalEntriesList) *createEntryImpl {
	var err error

	for _, entry := range entries {
		y, m, _ := entry.EntryTime.Date()
		month := time.Time{}
		month = month.AddDate(y, int(m), 0)

		balance := &AccountMonthlyBalance{
			Month:         month,
			AccountID:     entry.AccountID,
			JournalTeamID: entry.TeamID,
			Debit:         entry.Debit,
			Credit:        entry.Debit,
		}
		err = c.
			tx.
			Model(&AccountMonthlyBalance{}).
			Where("month = ?", month).
			Where("account_id = ?", balance.AccountID).
			Where("journal_team_id = ?", balance.JournalTeamID).
			Updates(map[string]interface{}{
				"debit":  gorm.Expr("debit + ?", balance.Debit),
				"credit": gorm.Expr("credit + ?", balance.Credit),
			}).
			Error

		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				err = c.
					tx.
					Save(balance).
					Error

				if err != nil {
					return c.setErr(err)
				}
			} else {
				return c.setErr(err)
			}
		}

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
func (c *createEntryImpl) To(account *EntryAccountPayload, amount float64) CreateEntry {
	acc, err := c.getAccount(account)
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

func (c *createEntryImpl) getAccount(accp *EntryAccountPayload) (*Account, error) {
	var acc Account
	var err error

	err = c.tx.Model(&Account{}).
		Where("account_key = ?", accp.Key).
		Where("team_id = ?", accp.TeamID).
		Find(&acc).
		Error

	if err != nil {
		return &acc, err
	}

	if acc.ID == 0 {
		return &acc, fmt.Errorf("account not found %s in team %d", accp.Key, accp.TeamID)
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

func NewCreateEntry(tx *gorm.DB, teamID uint) CreateEntry {
	return &createEntryImpl{
		tx:      tx,
		teamID:  teamID,
		entries: map[uint]*JournalEntry{},
	}
}

type journalEntriesImpl struct {
	tx *gorm.DB
}
