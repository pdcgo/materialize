package main

import (
	"time"

	"github.com/pdcgo/materialize/accounting_transaction/order_transaction"
	"github.com/pdcgo/materialize/database"
	"github.com/pdcgo/materialize/stat_process/gathering"
	"github.com/pdcgo/shared/db_models"
)

func main() {
	db, err := database.ConnectToProduction(1, "accounting_test")
	if err != nil {
		panic(err)
	}

	localdb, err := gathering.CreateDB()
	if err != nil {
		panic(err)
	}

	orders := []*db_models.Order{}

	err = db.
		Model(&db_models.Order{}).
		Preload("Items").
		Where("created_at > ?", time.Now().Local().AddDate(0, 0, -2)).
		Find(&orders).
		Error

	if err != nil {
		panic(err)
	}

	ordops := order_transaction.NewOrderTransaction(localdb)

	for _, order := range orders {

		err := ordops.CreateOrder(&order_transaction.CreateOrderPayload{
			TeamID:           order.TeamID,
			WarehouseID:      1,
			UserID:           order.OrderMpID,
			ShopID:           order.OrderMpID,
			OwnProductAmount: 12,
			CrossProductAmount: []*order_transaction.CrossProductAmount{
				{
					TeamID: 1,
					Amount: 123,
				},
			},
		})

		if err != nil {
			panic(err)
		}
	}

}
