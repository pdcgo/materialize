package main

import (
	"context"

	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
)

func registerCoder(ctx context.Context) error {
	return stat_replica.RegisterCoderSources(ctx,
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "teams", Schema: "public"},
			Coder: &models.Team{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "inv_resolutions", Schema: "public"},
			Coder: &models.InvResolution{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "expense_histories", Schema: "public"},
			Coder: &models.ExpenseHistory{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "restock_costs", Schema: "public"},
			Coder: &models.RestockCost{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "inv_transactions", Schema: "public"},
			Coder: &models.InvTransaction{},
		},
	)
}
