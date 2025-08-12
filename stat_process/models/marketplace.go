package models

import (
	"fmt"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/db_models"
)

type Marketplace struct {
	ID            uint `json:"id" gorm:"primarykey"`
	TeamID        uint `json:"team_id" gorm:"index:mp_team_id_unique,unique"`
	HoldAssetID   uint `json:"asset_id"`
	BankAccountID uint `json:"bank_account_id"`

	MpUsername  string                    `json:"mp_username" gorm:"index:mp_team_id_unique,unique"`
	MpName      string                    `json:"mp_name"`
	MpType      db_models.MarketplaceType `json:"mp_type" gorm:"index:mp_team_id_unique,unique"`
	Uri         string                    `json:"uri"`
	IsDuplicate bool                      `json:"is_duplicate"`
	Deleted     bool                      `json:"deleted" gorm:"index"`
}

// Key implements exact_one.ExactHaveKey.
func (m *Marketplace) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "marketplaces",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), m.ID)
}
