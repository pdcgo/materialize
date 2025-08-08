package main

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
)

type Team struct {
	ID                uint     `json:"id"`
	Type              string   `json:"type"`
	Name              string   `json:"name"`
	TeamCode          string   `json:"team_code"`
	Description       string   `json:"desc"`
	ProductLimitCount int      `json:"product_limit_count"`
	ProductCount      int      `json:"product_count"`
	InvoiceUnpaid     *float64 `json:"invoice_unpaid"`
	InvoiceNotFinal   *float64 `json:"invoice_not_final"`
	Deleted           bool     `json:"deleted"`
}

type WarehouseBundle struct {
	ID          uint
	BundleID    uint
	WarehouseID uint
	PairCount   int
}

// coder fix

func main() {
	ctx := context.Background()
	ctx = stat_replica.ContextWithCoder(ctx)
	stat_replica.RegisterCoderSource(ctx, &stat_replica.SourceMetadata{
		Table:  "warehouse_bundles",
		Schema: "public",
	}, &WarehouseBundle{})

	err := stat_replica.RegisterCoderSource(
		ctx,
		&stat_replica.SourceMetadata{
			Table:  "teams",
			Schema: "public",
		},
		&models.Team{},
	)

	if err != nil {
		panic(err)
	}

	conn, err := stat_replica.ConnectProdDatabase(ctx)
	if err != nil {
		panic(err)
	}

	defer conn.Close(ctx)

	// Initialize Replication
	initrep := stat_replica.NewInitReplica(ctx, conn)
	err = initrep.
		Initialize(true).
		Err()

	if err != nil {
		panic(err)
	}
	replication := stat_replica.NewReplication(ctx, conn)

	replication.AddHandler(func(msg *stat_replica.CdcMessage) {
		if msg == nil {
			return
		}

		if msg.Data == nil {
			return
		}

		log.Println(msg.SourceMetadata.Table, msg.Data)
	})

	err = replication.Start()
	if err != nil {
		panic(err)
	}

}

func ListTypesAndFields(data map[string]interface{}) {
	for key, val := range data {
		t := reflect.TypeOf(val)
		v := reflect.ValueOf(val)

		if t == nil {
			return
		}

		// Handle pointer
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			v = v.Elem()
		}

		fmt.Printf("Key: %s, Type: %s\n", key, t.String())

		if t.Kind() == reflect.Struct {
			fmt.Printf("  Struct fields for %s:\n", key)
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				fmt.Printf("    - %s (%s)\n", field.Name, field.Type)
			}
		}
	}
}
