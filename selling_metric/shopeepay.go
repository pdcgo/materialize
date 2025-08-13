package selling_metric

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
)

func NewDailyShopeepayBalanceMetric(
	badgedb *badger.DB,
	exact exact_one.ExactlyOnce,
) metric.MetricStore[*metric.DailyShopeepayBalance] {
	shopeeBalanceMetric := metric.NewDefaultMetricStore(
		badgedb,
		func() *metric.DailyShopeepayBalance {
			return &metric.DailyShopeepayBalance{}
		},
		func(data *metric.DailyShopeepayBalance) *metric.DailyShopeepayBalance {
			team := &models.Team{
				ID: data.TeamID,
			}

			exact.GetItemStruct(team)
			data.TeamName = team.Name

			data.DiffAmount = data.RefundAmount + data.TopupAmount - data.CostAmount
			data.ErrDiffAmount = data.ActualDiffAmount - data.DiffAmount
			return data
		},
	)

	return shopeeBalanceMetric
}
