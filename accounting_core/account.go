package accounting_core

import (
	"time"

	"gorm.io/gorm"
)

type createAccountImpl struct {
	tx *gorm.DB
}

// Create implements CreateAccount.
func (c *createAccountImpl) Create(tipe BalanceType, coa CoaCode, domainID uint, key AccountKey, name string) error {
	acc := Account{
		Key:         key,
		Name:        name,
		Coa:         coa,
		BalanceType: tipe,
		Created:     time.Now(),
	}

	err := c.tx.Save(&acc).Error
	return err
}

func NewCreateAccount(tx *gorm.DB) CreateAccount {
	return &createAccountImpl{
		tx: tx,
	}
}
