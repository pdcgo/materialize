package selling_pipeline

import (
	"log"

	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/yenstream"
)

func ExactOne(ctx *yenstream.RunnerContext, exact exact_one.ExactlyOnce, source yenstream.Pipeline) yenstream.Pipeline {

	exactstream := source.
		Via("save_to_badge_db", yenstream.NewFilter(ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {

			data, ok := cdata.Data.(exact_one.ExactHaveKey)
			if !ok {
				return false, nil
			}
			var found bool
			old := stat_replica.NewEmptyFromStruct(data).(exact_one.ExactHaveKey)
			change := exact.Change(data)
			err := change.
				Before(&found, old).
				Err()

			if err != nil {
				return false, err
			}

			switch cdata.ModType {
			case stat_replica.CdcBackfill:
				if found {
					return false, nil
				}

				err = change.
					Save().
					Err()

				if err != nil {
					return false, err
				}

			default:
				err = change.
					Save().
					Err()

				if err != nil {
					return false, err
				}
			}

			if found {
				cdata.OldData = old
			}
			if err != nil {
				log.Println(cdata.SourceMetadata.PrefixKey())
			}
			return true, err
		})).
		Via("enrich_order", yenstream.NewMap(ctx, func(cdata *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
			var err error
			switch cdata.SourceMetadata.Table {
			case "orders":
				data := cdata.Data.(*models.Order)

				inv := &models.InvOrderData{
					InvID:        data.InvertoryTxID,
					OrderID:      data.ID,
					TeamID:       data.TeamID,
					MpID:         data.OrderMpID,
					WarehouseFee: data.WarehouseFee,
					MpTotal:      float64(data.OrderMpTotal),
				}

				err = exact.Change(inv).Save().Err()
				if err != nil {
					return cdata, err
				}

				if data.InvertoryReturnTxID != 0 {
					inv := &models.InvOrderData{
						InvID:        data.InvertoryReturnTxID,
						OrderID:      data.ID,
						TeamID:       data.TeamID,
						MpID:         data.OrderMpID,
						WarehouseFee: data.WarehouseFee,
						MpTotal:      float64(data.OrderMpTotal),
					}

					err = exact.Change(inv).Save().Err()
					if err != nil {
						return cdata, err
					}
				}

			}

			return cdata, nil
		}))

	return exactstream

}
