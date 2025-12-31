package main

import (
	"context"
	"log"
	"log/slog"

	"github.com/pdcgo/materialize/stat_replica"
)

func main() {
	slog.Info("starting replication streaming")
	var err error

	ctx := context.Background()
	ctx = stat_replica.ContextWithCoder(ctx)

	repcfg := &stat_replica.ReplicationConfig{
		SlotName:        "stat_slot",
		PublicationName: "stat_publication",
		SlotTemporary:   true,
	}

	conn, err := stat_replica.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}

	defer conn.Close(ctx)

	// Initialize Replication
	initrep := stat_replica.NewInitReplica(ctx, conn, repcfg)
	err = initrep.
		Initialize(repcfg.SlotTemporary).
		Err()

	if err != nil {
		panic(err)
	}

	replication := stat_replica.NewReplication(ctx, conn, repcfg)
	replication.AddHandler(func(msg *stat_replica.CdcMessage) {
		if msg == nil {
			return
		}

		if msg.SourceMetadata.Table != "invertory_histories" {
			return
		}

		log.Println(msg)
	})

	err = replication.Start()
	if err != nil {
		panic(err)
	}
}
