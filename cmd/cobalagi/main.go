package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"time"

	"github.com/pdcgo/materialize/coders"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/wargasipil/stream_engine/stream_core"
)

func main() {
	slog.Info("starting replication streaming")
	var err error

	ctx := context.Background()
	ctx = stat_replica.ContextWithCoder(ctx)
	err = coders.WarehouseCoder(ctx)
	if err != nil {
		panic(err)
	}

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

	// bagian key value counter
	cfg := stream_core.NewDefaultCoreConfig()
	hashmap, err := stream_core.NewHashMapCounter(cfg)
	if err != nil {
		panic(err)
	}

	defer hashmap.Close()

	snapDuration := time.Second * 30
	tick := time.NewTimer(snapDuration)
	go func() {

		lastsnap := time.Now()
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				log.Println("snapshotting.....................................")
				hashmap.Snapshot(lastsnap, func(key string, kind reflect.Kind, value any) error {
					log.Println(key, value)
					return nil
				})
				lastsnap = time.Now()
				tick.Reset(snapDuration)

			}
		}

	}()

	replication := stat_replica.NewReplication(ctx, conn, repcfg)
	replication.AddHandler(func(msg *stat_replica.CdcMessage) {
		if msg == nil {
			return
		}

		if msg.SourceMetadata.Table != "invertory_histories" {
			return
		}

		data := (msg.Data).(*models.InvertoryHistory)
		skuData, _ := data.SkuID.Extract()

		// key := fmt.Sprintf("teams/%d/products/%d/warehouses/%d/stock_count", data.TeamID, skuData.ProductID, data.WarehouseID)

		var parentKey stream_core.NestedKey = stream_core.NestedKey(fmt.Sprintf("teams/%d/products/%d/warehouses/%d", skuData.TeamID, skuData.ProductID, skuData.WarehouseID))

		switch msg.ModType {
		case stat_replica.CdcDelete:

			// keyStock := fmt.Sprintf("inv_histories/%d/count", data.ID)
			// beforeStock := hashmap.GetInt64(keyStock)

			// keyStockAmount := fmt.Sprintf("inv_histories/%d/amount", data.ID)
			// beforeStockAmount := hashmap.GetFloat64(keyStockAmount)

			printdebug(data)

		case stat_replica.CdcInsert:
			if data.TxID == data.InTxID {
				hashmap.IncInt64("stock/inbound/count", int64(data.Count))
				hashmap.IncFloat64("stock/inbound/amount", (data.ExtPrice+data.Price)*float64(data.Count))
				hashmap.IncInt64("stock/current/count", int64(data.Count))
				hashmap.IncFloat64("stock/current/amount", (data.ExtPrice+data.Price)*float64(data.Count))

				parentKey.Iterate(func(key string) {
					hashmap.IncInt64(key+"/stock/inbound/count", int64(data.Count))
					hashmap.IncFloat64(key+"/stock/inbound/amount", (data.ExtPrice+data.Price)*float64(data.Count))
					hashmap.IncInt64(key+"/stock/current/count", int64(data.Count))
					hashmap.IncFloat64(key+"/stock/current/amount", (data.ExtPrice+data.Price)*float64(data.Count))
				})

				return
			}

			if data.Count > 0 {
				hashmap.IncInt64("stock/outbound/count", int64(data.Count))
				hashmap.IncFloat64("stock/outbound/amount", (data.ExtPrice+data.Price)*float64(data.Count))
				hashmap.IncInt64("stock/current/count", int64(data.Count))
				hashmap.IncFloat64("stock/current/amount", (data.ExtPrice+data.Price)*float64(data.Count))
				parentKey.Iterate(func(key string) {
					hashmap.IncInt64(key+"/stock/outbound/count", int64(data.Count))
					hashmap.IncFloat64(key+"/stock/outbound/amount", (data.ExtPrice+data.Price)*float64(data.Count))
					hashmap.IncInt64(key+"/stock/current/count", int64(data.Count))
					hashmap.IncFloat64(key+"/stock/current/amount", (data.ExtPrice+data.Price)*float64(data.Count))
				})
			} else {

			}

		case stat_replica.CdcUpdate:
			keyStock := fmt.Sprintf("inv_histories/%d/count", data.ID)
			beforeStock := hashmap.GetInt64(keyStock)
			nextStock := int64(data.Count * -1)
			var deltaStock int64 = nextStock - beforeStock
			hashmap.PutInt64(keyStock, nextStock)

			keyStockAmount := fmt.Sprintf("inv_histories/%d/amount", data.ID)
			beforeStockAmount := hashmap.GetFloat64(keyStockAmount)
			nextStockAmount := (data.ExtPrice + data.Price) * float64(data.Count) * -1
			var deltaStockAmount float64 = nextStockAmount - beforeStockAmount
			hashmap.PutFloat64(keyStockAmount, nextStockAmount)

			if deltaStock == 0 {
				return
			}

			hashmap.IncInt64("stock/current/count", deltaStock)
			hashmap.IncFloat64("stock/current/amount", deltaStockAmount)
			parentKey.Iterate(func(key string) {
				hashmap.IncInt64(key+"/stock/current/count", deltaStock)
				hashmap.IncFloat64(key+"/stock/current/amount", deltaStockAmount)
			})

			if deltaStock > 0 {
				hashmap.IncInt64("stock/inbound/count", deltaStock)
				hashmap.IncFloat64("stock/inbound/amount", deltaStockAmount)
				parentKey.Iterate(func(key string) {
					hashmap.IncInt64(key+"/stock/inbound/count", deltaStock)
					hashmap.IncFloat64(key+"/stock/inbound/amount", deltaStockAmount)
				})
			} else {
				hashmap.IncInt64("stock/outbound/count", deltaStock*-1)
				hashmap.IncFloat64("stock/outbound/amount", deltaStockAmount*-1)
				parentKey.Iterate(func(key string) {
					hashmap.IncInt64(key+"/stock/outbound/count", deltaStock*-1)
					hashmap.IncFloat64(key+"/stock/outbound/amount", deltaStockAmount*-1)
				})

			}

		}

	})

	err = replication.Start()
	if err != nil {
		panic(err)
	}
}

func printdebug(data any) {
	raw, _ := json.Marshal(data)
	log.Println(string(raw))
}
