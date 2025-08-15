package backfill

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type backfillOrderImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillOrderImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	rows, err := b.conn.Query(b.ctx, "SELECT * FROM orders WHERE date(created_at) > $1", start)
	if err != nil {
		return err
	}

	return RowParser(b.ctx, "orders", rows, handle)
}

func NewBackfillOrder(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}
	return &backfillOrderImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}

type backfillOrderAdjImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillOrderAdjImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	rows, err := b.conn.Query(b.ctx, "SELECT * FROM order_adjustments WHERE date(fund_at) > $1", start)
	if err != nil {
		return err
	}

	return RowParser(b.ctx, "order_adjustments", rows, handle)
}

func NewBackfillOrderAdj(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}

	return &backfillOrderAdjImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}
