package models

import (
	"fmt"
	"time"

	"github.com/pdcgo/materialize/stat_replica"
)

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
