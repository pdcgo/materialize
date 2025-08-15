package main

import (
	"context"
	"log"
	"time"

	"github.com/pdcgo/materialize/backfill_pipeline"
	"github.com/pdcgo/materialize/backfill_pipeline/backfill"
	"github.com/pdcgo/materialize/coders"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/stat_db"
	"github.com/pdcgo/materialize/stat_replica"
)

func main() {
	var err error
	ctx := context.Background()
	ctx = stat_replica.ContextWithCoder(ctx)

	err = coders.WarehouseCoder(ctx)

	badgedb, err := stat_db.NewBadgeDB("./streamdata/filltest")
	if err != nil {
		panic(err)
	}
	defer badgedb.Close()
	defer badgedb.DropAll()

	conn, err := backfill.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}
	exact := exact_one.NewBadgeExactOne(ctx, badgedb)
	back := backfill_pipeline.NewBackfillPipeline(
		ctx,
		conn,
		exact,
		&backfill.BackfillConfig{
			StartTime: time.Now().AddDate(0, -1, 0),
		},
	)

	cdataChan := make(chan *stat_replica.CdcMessage, 1)
	defer close(cdataChan)

	// dummy consuming
	go func() {
		for data := range cdataChan {
			log.Println(data)
		}
	}()

	err = back.
		Start(cdataChan).
		Err()

	if err != nil {
		panic(err)
	}
}
