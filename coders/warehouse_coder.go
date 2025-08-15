package coders

import (
	"context"

	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
)

func WarehouseCoder(ctx context.Context) error {
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
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "ads_expense_histories", Schema: "public"},
			Coder: &models.AdsExpenseHistory{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "expense_accounts", Schema: "public"},
			Coder: &models.ExpenseAccount{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "balance_account_histories", Schema: "public"},
			Coder: &models.BalanceAccountHistory{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "orders", Schema: "public"},
			Coder: &models.Order{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "marketplaces", Schema: "public"},
			Coder: &models.Marketplace{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "order_adjustments", Schema: "public"},
			Coder: &models.OrderAdjustment{},
		},
		&stat_replica.CoderReg{
			Meta:  &stat_replica.SourceMetadata{Table: "order_timestamps", Schema: "public"},
			Coder: &models.OrderTimestamp{},
		},
	)
}
