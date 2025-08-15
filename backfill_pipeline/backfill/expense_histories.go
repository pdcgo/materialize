package backfill

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type backfillExpenseHistImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillExpenseHistImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	rows, err := b.conn.Query(b.ctx, "SELECT * FROM expense_histories WHERE date(at AT TIME ZONE 'Asia/Jakarta') > $1", start)
	if err != nil {
		return err
	}

	return RowParser(b.ctx, "expense_histories", rows, handle)
}

func NewBackfillExpenseHist(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}
	return &backfillExpenseHistImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}

type backfillAdsExpenseHistImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillAdsExpenseHistImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	rows, err := b.conn.Query(b.ctx, "SELECT * FROM ads_expense_histories WHERE date(at AT TIME ZONE 'Asia/Jakarta') > $1", start)
	if err != nil {
		return err
	}

	return RowParser(b.ctx, "ads_expense_histories", rows, handle)
}

func NewBackfillAdsExpenseHist(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}
	return &backfillAdsExpenseHistImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}
