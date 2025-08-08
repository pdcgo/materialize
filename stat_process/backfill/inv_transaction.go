package backfill

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type backfillInvTransactionImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillInvTransactionImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	rows, err := b.conn.Query(b.ctx, "select * from inv_transactions it  WHERE date(it.created AT TIME ZONE 'Asia/Jakarta') > $1", start)
	if err != nil {
		return err
	}

	// return
	return RowParser(b.ctx, "inv_transactions", rows, handle)
}

func NewBackfillInvTransaction(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}
	return &backfillInvTransactionImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}
