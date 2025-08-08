package metric_test

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/stretchr/testify/assert"
)

type Tcount struct {
	ID     uint    `json:"id"`
	Day    string  `json:"day"`
	Amount float64 `json:"amount"`
}

// Key implements metric.MetricData.
func (t *Tcount) Key() string {
	return fmt.Sprintf("%d", t.ID)
}

func TestMetricStore(t *testing.T) {
	var db db_mock.BadgeDBMock
	moretest.Suite(t, "testing metric store",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&db),
		},
		func(t *testing.T) {
			tmetric := metric.NewDefaultMetricStore(db.DB, func() *Tcount {
				return &Tcount{}
			}, nil)

			for _, c := range []uint{1, 2, 3, 3, 2, 1, 1, 1, 1, 1} {
				td := &Tcount{
					ID:     c,
					Day:    "2025-08-01",
					Amount: 1,
				}
				err := tmetric.Merge(fmt.Sprintf("%d", td.ID), func(acc *Tcount) *Tcount {
					if acc == nil {
						return td
					}
					acc.Amount += td.Amount
					return acc
				})

				assert.Nil(t, err)
			}

			tmetric.FlushHandler(func(acc *Tcount) error {
				raw, _ := json.Marshal(acc)
				log.Println(string(raw))
				return nil
			})

		},
	)
}
