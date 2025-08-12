package main

import (
	"context"
	"log/slog"

	"github.com/pdcgo/materialize/stat_process/backfill"
	"github.com/pdcgo/materialize/stat_replica"
)

type StreamStatus string

const (
	BackfillMode StreamStatus = "backfill"
	ReplicaMode  StreamStatus = "replica"
)

type CDCStream interface {
	Init() CDCStream
	GetStatus() StreamStatus
	AddTearDown(handle func()) CDCStream
	Close()
	Backfill() CDCStream
	Stream() CDCStream
	Err() error
}

type cdcStreamImpl struct {
	status    StreamStatus
	repcfg    *stat_replica.ReplicationConfig
	cfg       *backfill.BackfillConfig
	ctx       context.Context
	cdataChan chan *stat_replica.CdcMessage
	err       error
	closefunc []func()
}

// GetStatus implements CDCStream.
func (c *cdcStreamImpl) GetStatus() StreamStatus {
	return c.status
}

// AddTearDown implements CDCStream.
func (c *cdcStreamImpl) AddTearDown(handle func()) CDCStream {
	c.closefunc = append(c.closefunc, handle)
	return c
}

// Close implements CDCStream.
func (c *cdcStreamImpl) Close() {

	for i := len(c.closefunc); i >= 0; i-- {
		c.closefunc[i]()
	}
}

// Backfill implements CDCStream.
func (c *cdcStreamImpl) Backfill() CDCStream {
	slog.Info("starting backfilling process")

	var err error
	c.status = BackfillMode
	conn, err := backfill.ConnectProdDatabase(c.ctx)
	if err != nil {
		return c.setErr(err)
	}

	defer conn.Close(c.ctx)

	tables := backfill.NewBackfillTable(c.ctx, conn, []string{
		"teams",
		"expense_accounts",
		"marketplaces",
	})
	err = tables.Start(func(cdata *stat_replica.CdcMessage) {
		c.cdataChan <- cdata
	})
	if err != nil {
		return c.setErr(err)
	}

	// invtx := backfill.NewBackfillInvTransaction(c.ctx, conn, c.cfg)
	// invtx.Start(func(cdata *stat_replica.CdcMessage) {
	// 	c.cdataChan <- cdata
	// })

	// restockCost := backfill.NewBackfillRestockCost(c.ctx, conn, c.cfg)
	// restockCost.Start(func(cdata *stat_replica.CdcMessage) {
	// 	c.cdataChan <- cdata
	// })

	// balanceHist := backfill.NewBackfillBalanceHistories(c.ctx, conn, c.cfg)
	// balanceHist.Start(func(cdata *stat_replica.CdcMessage) {
	// 	c.cdataChan <- cdata
	// })

	adsExpense := backfill.NewBackfillAdsExpenseHist(c.ctx, conn, c.cfg)
	adsExpense.Start(func(cdata *stat_replica.CdcMessage) {
		c.cdataChan <- cdata
	})

	order := backfill.NewBackfillOrder(c.ctx, conn, c.cfg)
	order.Start(func(cdata *stat_replica.CdcMessage) {
		c.cdataChan <- cdata
	})

	orderadj := backfill.NewBackfillOrderAdj(c.ctx, conn, c.cfg)
	orderadj.Start(func(cdata *stat_replica.CdcMessage) {
		c.cdataChan <- cdata
	})

	// expense := backfill.NewBackfillExpenseHist(c.ctx, conn, c.cfg)
	// expense.Start(func(cdata *stat_replica.CdcMessage) {
	// 	c.cdataChan <- cdata
	// })

	// invres := backfill.NewBackfillInvResolutionHist(c.ctx, conn, c.cfg)
	// invres.Start(func(cdata *stat_replica.CdcMessage) {
	// 	c.cdataChan <- cdata
	// })

	return c
}

// Err implements CDCStream.
func (c *cdcStreamImpl) Err() error {
	return c.err
}

func (c *cdcStreamImpl) setErr(err error) *cdcStreamImpl {
	if c.err != nil {
		return c
	}

	if err != nil {
		c.err = err
	}

	return c
}

// Init implements CDCStream.
func (c *cdcStreamImpl) Init() CDCStream {
	return c
}

// Stream implements CDCStream.
func (c *cdcStreamImpl) Stream() CDCStream {

	slog.Info("starting replication streaming")
	var err error
	c.status = ReplicaMode
	conn, err := stat_replica.ConnectProdDatabase(c.ctx)
	if err != nil {
		return c.setErr(err)
	}

	defer conn.Close(c.ctx)

	// Initialize Replication
	initrep := stat_replica.NewInitReplica(c.ctx, conn, c.repcfg)
	err = initrep.
		Initialize(c.repcfg.SlotTemporary).
		Err()

	if err != nil {
		c.setErr(err)
	}

	replication := stat_replica.NewReplication(c.ctx, conn, c.repcfg)
	replication.AddHandler(func(msg *stat_replica.CdcMessage) {
		if msg == nil {
			return
		}

		switch msg.SourceMetadata.Table {
		case "stat_restocks", "order_tag_relations":
			return
		}

		c.cdataChan <- msg
	})

	err = replication.Start()
	if err != nil {
		return c.setErr(err)
	}

	return c
}

func NewCDCStream(ctx context.Context, cfg *backfill.BackfillConfig, repcfg *stat_replica.ReplicationConfig, cdataChan chan *stat_replica.CdcMessage) CDCStream {
	return &cdcStreamImpl{
		repcfg:    repcfg,
		ctx:       ctx,
		cfg:       cfg,
		cdataChan: cdataChan,
	}
}
