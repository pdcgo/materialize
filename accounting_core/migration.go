package accounting_core

import "gorm.io/gorm"

func GormAutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&Account{},
		&JournalEntry{},
		&Transaction{},
		&Label{},
		&TransactionLabel{},
	)
}
