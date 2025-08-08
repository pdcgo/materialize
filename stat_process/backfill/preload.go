package backfill

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

type backfillTableImpl struct {
	ctx    context.Context
	conn   *pgx.Conn
	tables []string
}

// Start implements Backfill.
func (b *backfillTableImpl) Start(handle BackfillHandle) error {
	var err error

	for _, tablename := range b.tables {
		slog.Info("backfilling table", slog.String("table", tablename))
		var rows pgx.Rows
		rows, err = b.conn.Query(b.ctx, fmt.Sprintf("SELECT * FROM %s", tablename))
		if err != nil {
			return err
		}

		err = RowParser(b.ctx, tablename, rows, handle)
		if err != nil {
			slog.Error(err.Error())
			return err
		}
	}

	return err
}

func NewBackfillTable(
	ctx context.Context,
	conn *pgx.Conn,
	tables []string,
) Backfill {
	return &backfillTableImpl{
		ctx:    ctx,
		conn:   conn,
		tables: tables,
	}
}
