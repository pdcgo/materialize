package backfill

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type backfillInvoiceImpl struct {
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillInvoiceImpl) Start(handle BackfillHandle) error {
	rows, err := b.conn.Query(b.ctx, "SELECT * FROM invoices WHERE date(created) = '2025-08-01'")
	if err != nil {
		return err
	}

	return RowParser(b.ctx, "invoices", rows, handle)
}

func NewBackfillInvoice(ctx context.Context, conn *pgx.Conn) Backfill {
	return &backfillInvoiceImpl{
		ctx:  ctx,
		conn: conn,
	}
}
