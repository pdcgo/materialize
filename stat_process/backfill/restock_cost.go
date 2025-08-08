package backfill

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type backfillRestockCostImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillRestockCostImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	query := `
select 
	rc.id, 	
	rc.inv_transaction_id,
	rc.shipping_fee,
	rc.other_fee,
	rc.per_piece_fee,
	rc.payment_type,
	rc.cod_fee
from restock_costs rc join inv_transactions it on it.id = rc.inv_transaction_id
where 
	date(it.created AT TIME ZONE 'Asia/Jakarta') > $1
	`
	rows, err := b.conn.Query(b.ctx, query, start)
	if err != nil {
		return err
	}

	// return

	return RowParser(b.ctx, "restock_costs", rows, handle)
}

func NewBackfillRestockCost(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}
	return &backfillRestockCostImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}
