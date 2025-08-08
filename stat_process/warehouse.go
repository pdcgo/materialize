package stat_process

import (
	"context"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/yenstream"
)

func WarehouseMetric(ctx context.Context, cdcin chan *stat_replica.CdcMessage) error {
	// var exact ExactlyOnce
	yenstream.
		NewRunnerContext(ctx).
		CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
			source := yenstream.NewChannelSource(ctx, cdcin)

			return source.
				Via("exactly once data", yenstream.NewFilter(ctx, func(data *stat_replica.CdcMessage) (bool, error) {
					// exact.AddItem()
					return false, nil
				})).
				Via("empty map", yenstream.NewMap(ctx, func(data *stat_replica.CdcMessage) (*stat_replica.CdcMessage, error) {
					return data, nil
				}))
		})

	return nil
}
