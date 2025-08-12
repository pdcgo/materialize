package main

import (
	"github.com/pdcgo/materialize/stat_process/exact_one"
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
			err := exact.
				Change(data).
				Before(&found, old).
				Save().
				Err()

			if found {
				cdata.OldData = old
			}

			return true, err
		}))

	return exactstream

}
