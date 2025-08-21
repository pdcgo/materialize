package accounting_core

import (
	"errors"
	"time"

	"gorm.io/gorm"
)

type CreateTransaction interface {
	Labels(labels []*Label) CreateTransaction
	Create(tran *Transaction) CreateTransaction
	Err() error
}

type createTansactionImpl struct {
	tx   *gorm.DB
	err  error
	tran *Transaction
}

// Create implements CreateTransaction.
func (c *createTansactionImpl) Create(tran *Transaction) CreateTransaction {
	tran.Created = time.Now()
	err := c.tx.Save(tran).Error
	if err != nil {
		return c.setErr(err)
	}

	c.tran = tran

	return c
}

// Err implements CreateTransaction.
func (c *createTansactionImpl) Err() error {
	return c.err
}

// Labels implements CreateTransaction.
func (c *createTansactionImpl) Labels(labels []*Label) CreateTransaction {
	if c.tran == nil {
		return c.setErr(errors.New("transaction nil"))
	}

	if c.tran.ID == 0 {
		return c.setErr(errors.New("transaction id is null"))
	}

	var err error
	for _, label := range labels {
		keyID := label.Hash()
		err = c.tx.Model(&Label{}).Where("id = ?", keyID).First(label).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				err = c.tx.Save(label).Error
				if err != nil {
					return c.setErr(err)
				}
			} else {
				return c.setErr(err)
			}

		}

		rel := TransactionLabel{
			TransactionID: c.tran.ID,
			LabelID:       label.ID,
		}

		err = c.tx.Save(&rel).Error
		if err != nil {
			return c.setErr(err)
		}
	}

	return c
}
func (c *createTansactionImpl) setErr(err error) *createTansactionImpl {
	if c.err != nil {
		return c
	}

	if err != nil {
		c.err = err
	}

	return c
}

func NewTransaction(tx *gorm.DB) CreateTransaction {
	return &createTansactionImpl{
		tx: tx,
	}
}
