package backfill

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type backfillInvResolutionImpl struct {
	cfg  *BackfillConfig
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillInvResolutionImpl) Start(handle BackfillHandle) error {
	start := b.cfg.StartTime.Format("2006-01-02")
	rows, err := b.conn.Query(b.ctx, "SELECT * FROM inv_resolutions WHERE date(fund_at AT TIME ZONE 'Asia/Jakarta') > $1", start)
	if err != nil {
		return err
	}

	// return

	return RowParser(b.ctx, "inv_resolutions", rows, handle)
}

func NewBackfillInvResolutionHist(ctx context.Context, conn *pgx.Conn, cfg *BackfillConfig) Backfill {
	if cfg == nil {
		cfg = DefaultBackfillConfig
	}
	return &backfillInvResolutionImpl{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}
