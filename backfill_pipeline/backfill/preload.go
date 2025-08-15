package backfill

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/pdcgo/materialize/stat_replica"
)

func SideloadQuery(
	ctx context.Context,
	conn *pgx.Conn,
	tablename string,
	query string,
	args ...any,
) ([]*stat_replica.CdcMessage, error) {
	var err error
	var result []*stat_replica.CdcMessage
	var rows pgx.Rows
	rows, err = conn.Query(ctx, query, args...)
	if err != nil {
		return result, err
	}

	err = RowParser(ctx, tablename, rows, func(cddata *stat_replica.CdcMessage) {
		result = append(result, cddata)
	})
	if err != nil {
		return result, err
	}

	return result, err
}

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
