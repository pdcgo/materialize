package backfill

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pdcgo/materialize/stat_replica"
)

type backfillOrderImpl struct {
	ctx  context.Context
	conn *pgx.Conn
}

// Start implements Backfill.
func (b *backfillOrderImpl) Start(handle BackfillHandle) error {

	rows, err := b.conn.Query(b.ctx, "SELECT * FROM orders WHERE date(created_at) = '2025-08-01'")
	if err != nil {
		return err
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	numCols := len(fieldDescriptions)

	for rows.Next() {
		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		rowMap := make(map[string]interface{}, numCols)
		for i, fd := range fieldDescriptions {
			colName := string(fd.Name)
			rowMap[colName] = values[i]
		}

		cddata := stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:    "orders",
				Schema:   "public",
				Database: "",
			},
			ModType:   stat_replica.CdcBackfill,
			Data:      rowMap,
			Timestamp: time.Now().UnixMicro(),
		}

		handle(&cddata)
	}

	return rows.Err()
}

func NewBackfillOrder(ctx context.Context, conn *pgx.Conn) Backfill {
	return &backfillOrderImpl{
		ctx:  ctx,
		conn: conn,
	}
}
