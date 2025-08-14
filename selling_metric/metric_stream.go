package selling_metric

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

type MetricStream interface {
	DataChanges(db *badger.DB) yenstream.Pipeline
	CounterChanges() yenstream.Pipeline
}

type metricStreamImpl[R metric.MetricData] struct {
	ctx    *yenstream.RunnerContext
	metric metric.MetricStore[R]
	pipe   yenstream.Pipeline
}

// CounterChanges implements MetricStream.
func (m *metricStreamImpl[R]) CounterChanges() yenstream.Pipeline {
	return m.pipe
}

// DataChanges implements MetricStream.
func (m *metricStreamImpl[R]) DataChanges(db *badger.DB) yenstream.Pipeline {
	return m.
		pipe.
		Via("data_change_metric", yenstream.NewMap(m.ctx, func(data metric.MetricData) (metric.MetricData, error) {
			var old R
			var err error
			old, err = m.getItem(db, data.Key())
			if err != nil {
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return data, err
				}
			}
			newd := data.Merge(old)
			err = m.setItem(db, newd.Key(), newd.(R))
			if err != nil {
				return data, err
			}
			data = m.metric.Output(newd.(R))
			return data, nil
		}))
}

func (m *metricStreamImpl[R]) getItem(db *badger.DB, key string) (R, error) {
	acc := m.metric.EmptyAccumulator()

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return json.Unmarshal(val, acc)
	})

	return acc, err
}

func (m *metricStreamImpl[R]) setItem(db *badger.DB, key string, data R) error {
	var err error
	err = db.Update(func(txn *badger.Txn) error {
		var raw []byte
		raw, err = json.Marshal(data)
		if err != nil {
			return err
		}
		err = txn.Set([]byte(key), raw)
		return err
	})

	return err
}

func NewMetricStream[R metric.MetricData](
	ctx *yenstream.RunnerContext,
	flushtime time.Duration,
	met metric.MetricStore[R],
	spipe yenstream.Pipeline,
) MetricStream {
	var pipe yenstream.Pipeline = spipe.
		Via(met.Name(),
			&metricGather[R]{
				ctx:           ctx,
				flushDuration: flushtime,
				in:            make(chan any, 1),
				out:           yenstream.NewNodeOut(ctx),
				metric:        met,
			},
		)

	return &metricStreamImpl[R]{
		ctx:    ctx,
		metric: met,
		pipe:   pipe,
	}
}

type metricGather[R metric.MetricData] struct {
	ctx           *yenstream.RunnerContext
	label         string
	in            chan any
	out           yenstream.NodeOut
	flushDuration time.Duration
	metric        metric.MetricStore[R]
}

// In implements yenstream.Pipeline.
func (m *metricGather[R]) In() chan any {
	return m.in
}

// Out implements yenstream.Pipeline.
func (m *metricGather[R]) Out() yenstream.NodeOut {
	return m.out
}

// Process implements yenstream.Pipeline.
func (m *metricGather[R]) Process() {
	out := m.out.C()
	defer close(out)

	flushd := time.NewTimer(m.flushDuration)
	defer flushd.Stop()

Parent:
	for {
		select {
		case _, ok := <-m.in:
			if !ok {
				break Parent
			}

		case <-flushd.C:
			m.flushData(out)
			flushd.Reset(m.flushDuration)
		}
	}

	m.flushData(out)
}

func (m *metricGather[R]) flushData(out chan any) {
	m.metric.Change(func(acc R) {
		out <- acc
	})
}

// SetLabel implements yenstream.Pipeline.
func (m *metricGather[R]) SetLabel(label string) {
	m.label = label
}

// Via implements yenstream.Pipeline.
func (m *metricGather[R]) Via(label string, pipe yenstream.Pipeline) yenstream.Pipeline {
	m.ctx.RegisterStream(label, m, pipe)
	return pipe
}
