package backfill_pipeline

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pdcgo/materialize/backfill_pipeline/backfill"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/yenstream"
)

type BackfillPipeline interface {
	Start(outdataChan chan *stat_replica.CdcMessage) BackfillPipeline
	Err() error
}

type backfillImpl struct {
	ctx       context.Context
	conn      *pgx.Conn
	exact     exact_one.ExactlyOnce
	cdataChan chan *stat_replica.CdcMessage
	cfg       *backfill.BackfillConfig
	err       error
}

// Err implements BackfillPipeline.
func (b *backfillImpl) Err() error {
	return b.err
}

// Start implements BackfillPipeline.
func (b *backfillImpl) Start(outdataChan chan *stat_replica.CdcMessage) BackfillPipeline {
	go b.preload()
	yenstream.
		NewRunnerContext(b.ctx).
		CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
			source := yenstream.NewChannelSource(ctx, b.cdataChan)

			return source.
				Via("debug", yenstream.NewMap(ctx, func(data any) (any, error) {
					// raw, _ := json.Marshal(data)
					// log.Println(string(raw))
					return data, nil
				}))

		})

	return b

}

func (b *backfillImpl) preload() {
	var err error
	defer close(b.cdataChan)

	tables := backfill.NewBackfillTable(b.ctx, b.conn, []string{
		"teams",
		"expense_accounts",
		"marketplaces",
	})
	err = tables.Start(func(cdata *stat_replica.CdcMessage) {
		b.cdataChan <- cdata
	})
	if err != nil {
		b.setErr(err)
		return
	}

	order := backfill.NewBackfillOrder(b.ctx, b.conn, b.cfg)
	order.Start(func(cdata *stat_replica.CdcMessage) {
		b.cdataChan <- cdata
	})

	orderadj := backfill.NewBackfillOrderAdj(b.ctx, b.conn, b.cfg)
	orderadj.Start(func(cdata *stat_replica.CdcMessage) {
		b.cdataChan <- cdata
	})

}

func (b *backfillImpl) setErr(err error) *backfillImpl {
	if b.err != nil {
		return b
	}

	if err != nil {
		b.err = err
	}

	return b
}

func NewBackfillPipeline(
	ctx context.Context,
	conn *pgx.Conn,
	exact exact_one.ExactlyOnce,
	cfg *backfill.BackfillConfig,
) BackfillPipeline {
	return &backfillImpl{
		cfg:       cfg,
		exact:     exact,
		ctx:       ctx,
		conn:      conn,
		cdataChan: make(chan *stat_replica.CdcMessage, 1),
	}
}
