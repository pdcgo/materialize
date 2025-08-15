package backfill

import (
	"time"

	"github.com/pdcgo/materialize/stat_replica"
)

type BackfillHandle func(cdata *stat_replica.CdcMessage)

type Backfill interface {
	Start(handle BackfillHandle) error
}

type BackfillConfig struct {
	StartTime time.Time
}

var DefaultBackfillConfig = &BackfillConfig{
	StartTime: time.Now().AddDate(0, -2, 0),
}
