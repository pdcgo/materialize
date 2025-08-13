package selling_metric

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

// var _ yenstream.Source = (*metricImpl[metric.MetricData])(nil)
// var _ Metric[metric.MetricData] = (*metricImpl[metric.MetricData])(nil)

type metricImpl[R metric.MetricData] struct {
	sync.Mutex
	flushdur time.Duration
	ctx      *yenstream.RunnerContext
	exact    exact_one.ExactlyOnce
	cacc     func() R
	output   func(r R) (R, error)
	db       *badger.DB
	out      yenstream.NodeOut
	datas    map[string]R
	label    string
}

// EmptyAccumulator implements Metric.
func (m *metricImpl[R]) EmptyAccumulator() R {
	return m.cacc()
}

// Process implements Outlet.
func (m *metricImpl[R]) Process() {

	out := m.out.C()
	defer close(out)

	flushd := time.NewTimer(m.flushdur)
	defer flushd.Stop()

Parent:
	for {
		select {
		case <-m.ctx.Done():
			break Parent

		case <-flushd.C:
			m.flushData(out)
			flushd.Reset(m.flushdur)
		}
	}

	m.flushData(out)
}

func (m *metricImpl[R]) flushData(out chan any) {
	m.Lock()
	datas := m.datas
	m.datas = map[string]R{}
	m.Unlock()

	for _, item := range datas {
		data, err := m.output(item)
		if err != nil {
			slog.Error(err.Error(), slog.String("label", m.label))
		}

		out <- data
	}
}

// Merge implements Metric.
func (m *metricImpl[R]) Merge(key string, merger func(acc R) R) error {
	m.Lock()
	defer m.Unlock()

	var err error
	var ok bool
	var acc R

	acc, ok = m.datas[key]
	if !ok {
		acc, err = m.getItem(key)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
	}

	newacc := merger(acc)
	m.datas[key] = newacc
	return nil
}

func (m *metricImpl[R]) getItem(key string) (R, error) {
	acc := m.EmptyAccumulator()

	err := m.db.View(func(txn *badger.Txn) error {
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

// func (m *metricImpl[R]) setItem(key string, data R) error {
// 	var err error
// 	err = m.db.Update(func(txn *badger.Txn) error {
// 		var raw []byte
// 		raw, err = json.Marshal(data)
// 		if err != nil {
// 			return err
// 		}
// 		err = txn.Set([]byte(key), raw)
// 		return err
// 	})

// 	return err
// }

// Out implements yenstream.Source.
func (m *metricImpl[R]) Out() yenstream.NodeOut {
	return m.out
}

// Via implements yenstream.Source.
func (m *metricImpl[R]) Via(label string, pipe yenstream.Pipeline) yenstream.Pipeline {
	m.ctx.RegisterStream(label, m, pipe)
	return pipe
}

func NewMetric[R metric.MetricData](
	label string,
	ctx *yenstream.RunnerContext,
	db *badger.DB,
	exact exact_one.ExactlyOnce,
	flushdur time.Duration,
	emptyAcc func() R,
	output func(R) (R, error),
) *metricImpl[R] {
	if output == nil {
		output = func(r R) (R, error) {
			return r, nil
		}
	}

	source := &metricImpl[R]{
		label:    label,
		flushdur: flushdur,
		ctx:      ctx,
		exact:    exact,
		cacc:     emptyAcc,
		output:   output,
		db:       db,
		out:      yenstream.NewNodeOut(ctx),
		datas:    map[string]R{},
	}

	ctx.AddProcess(source.Process)
	return source
}
