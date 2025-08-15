package backfill_pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pdcgo/materialize/backfill_pipeline/backfill"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/yenstream"
)

type Sideload struct {
	sync.Mutex
	ctx   *yenstream.RunnerContext
	conn  *pgx.Conn
	exact exact_one.ExactlyOnce
}

func (s *Sideload) All(source yenstream.Pipeline) yenstream.Pipeline {
	tampered := map[string]bool{
		"inv_transactions":  true,
		"order_adjustments": true,
	}
	normalSource := source.
		Via("filter_tampered", yenstream.NewFilter(s.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if tampered[cdata.SourceMetadata.Table] {
				return false, nil
			}
			return true, nil
		}))

	orderadj := source.
		Via("sideload_filter_ord_adjustment", yenstream.NewFilter(s.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "order_adjustments" {
				return false, nil
			}

			return true, nil
		})).
		Via("sideload_adj_batch", yenstream.NewBatch[*stat_replica.CdcMessage](s.ctx, 200, time.Second*5)).
		Via("adj_sideload_ord", yenstream.NewFlatMap(s.ctx,
			func(cdatas []*stat_replica.CdcMessage) ([]*stat_replica.CdcMessage, error) {
				var err error
				var result []*stat_replica.CdcMessage

				orderIDs := []uint{}
				for _, cdata := range cdatas {
					d := cdata.Data.(*models.OrderAdjustment)

					ord := &models.Order{
						ID: d.OrderID,
					}

					found, err := s.exact.GetItemStruct(ord)
					if err != nil {
						return result, err
					}

					if !found {
						orderIDs = append(orderIDs, d.OrderID)
					}
				}

				scdatas, err := s.getOrderByIDs(orderIDs)
				if err != nil {
					return result, err
				}

				result = append(result, scdatas...)
				result = append(result, cdatas...)

				return result, err
			}))

	invtx := source.
		Via("filter_inv_transaction", yenstream.NewFilter(s.ctx, func(cdata *stat_replica.CdcMessage) (bool, error) {
			if cdata.SourceMetadata.Table != "inv_transactions" {
				return false, nil
			}
			return true, nil
		})).
		Via("inv_batching", yenstream.NewBatch[*stat_replica.CdcMessage](s.ctx, 200, time.Second*5)).
		Via("sideloading_order", yenstream.NewFlatMap(s.ctx,
			func(cdatas []*stat_replica.CdcMessage) ([]*stat_replica.CdcMessage, error) {
				var err error
				var result []*stat_replica.CdcMessage

				invIDs := []uint{}
				for _, cdata := range cdatas {
					d := cdata.Data.(*models.InvTransaction)

					ordinv := &models.InvOrderData{
						InvID: d.ID,
					}

					found, err := s.exact.GetItemStruct(ordinv)
					if err != nil {
						return result, err
					}

					if !found {
						invIDs = append(invIDs, d.ID)
					}
				}

				if len(invIDs) == 0 {
					result = append(result, cdatas...)
					return result, err
				}

				scdatas, err := s.getOrderByTxIDs(invIDs)

				if err != nil {
					return result, err
				}

				// log.Println(scdatas)

				result = append(result, scdatas...)
				result = append(result, cdatas...)

				return result, err
			}))

	return yenstream.NewFlatten(s.ctx, "flatenningSideload",
		invtx,
		orderadj,
		normalSource,
	)

}

func (s *Sideload) getOrderByIDs(orderIDs []uint) ([]*stat_replica.CdcMessage, error) {
	s.Lock()
	defer s.Unlock()
	orderIDs = Unique(orderIDs)
	scdatas, err := backfill.SideloadQuery(
		s.ctx,
		s.conn,
		"orders",
		fmt.Sprintf(
			"select * from orders o where o.id in (%s)",
			UintSliceToString(orderIDs),
		),
	)

	return scdatas, err
}

func (s *Sideload) getOrderByTxIDs(txIDs []uint) ([]*stat_replica.CdcMessage, error) {
	s.Lock()
	defer s.Unlock()
	txIDs = Unique(txIDs)
	scdatas, err := backfill.SideloadQuery(
		s.ctx,
		s.conn,
		"orders",
		fmt.Sprintf(
			"select * from orders o where o.invertory_tx_id in (%s) or o.invertory_return_tx_id in (%s)",
			UintSliceToString(txIDs),
			UintSliceToString(txIDs),
		),
	)

	return scdatas, err
}

func NewSideload(ctx *yenstream.RunnerContext, conn *pgx.Conn, exact exact_one.ExactlyOnce) *Sideload {

	return &Sideload{
		ctx:   ctx,
		conn:  conn,
		exact: exact,
	}
}

func Unique[T comparable](input []T) []T {
	seen := make(map[T]struct{})
	result := make([]T, 0, len(input))
	for _, v := range input {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

func UintSliceToString(nums []uint) string {
	strs := make([]string, len(nums))
	for i, n := range nums {
		strs[i] = strconv.FormatUint(uint64(n), 10)
	}
	return strings.Join(strs, ",")
}
