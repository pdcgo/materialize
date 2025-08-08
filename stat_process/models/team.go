package models

import (
	"fmt"

	"github.com/pdcgo/materialize/stat_replica"
)

type Team struct {
	ID          uint   `json:"id"`
	Type        string `json:"type"`
	Name        string `json:"name"`
	TeamCode    string `json:"team_code"`
	Description string `json:"desc"`
	Deleted     bool   `json:"deleted"`
}

// Key implements exact_one.ExactHaveKey.
func (t *Team) Key() string {
	meta := stat_replica.SourceMetadata{
		Table:  "teams",
		Schema: "public",
	}
	return fmt.Sprintf("%s%d", meta.PrefixKey(), t.ID)
}
