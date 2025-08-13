package selling_metric

import (
	"time"

	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

// var _ yenstream.Pipeline = (*metricStreamImpl)(nil)

type metricStreamImpl[R metric.MetricData] struct {
	ctx           *yenstream.RunnerContext
	label         string
	out           yenstream.NodeOut
	in            chan any
	flushDuration time.Duration
	met           metric.MetricStore[R]
}

// In implements yenstream.Pipeline.
func (m *metricStreamImpl[R]) In() chan any {
	return m.in
}

// Out implements yenstream.Pipeline.
func (m *metricStreamImpl[R]) Out() yenstream.NodeOut {
	return m.out
}

// Process implements yenstream.Pipeline.
func (m *metricStreamImpl[R]) Process() {
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

func (m *metricStreamImpl[R]) flushData(out chan any) {
	m.met.Change(func(acc R) {
		out <- acc
	})
}

// SetLabel implements yenstream.Pipeline.
func (m *metricStreamImpl[R]) SetLabel(label string) {
	m.label = label
}

// Via implements yenstream.Pipeline.
func (m *metricStreamImpl[R]) Via(label string, pipe yenstream.Pipeline) yenstream.Pipeline {
	m.ctx.RegisterStream(label, m, pipe)

	return pipe
}

func NewMetricStream[R metric.MetricData](
	ctx *yenstream.RunnerContext,
	flushDuration time.Duration,
	met metric.MetricStore[R],
) *metricStreamImpl[R] {
	return &metricStreamImpl[R]{
		flushDuration: flushDuration,
		ctx:           ctx,
		out:           yenstream.NewNodeOut(ctx),
		in:            make(chan any, 1),
		met:           met,
	}
}
