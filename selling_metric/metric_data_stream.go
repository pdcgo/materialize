package selling_metric

import (
	"time"

	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/shared/yenstream"
)

// var _ yenstream.Pipeline = (*metricStreamImpl)(nil)

type metricDataStreamImpl[R metric.MetricData] struct {
	ctx           *yenstream.RunnerContext
	label         string
	out           yenstream.NodeOut
	in            chan any
	flushDuration time.Duration
	met           metric.MetricStore[R]
}

// In implements yenstream.Pipeline.
func (m *metricDataStreamImpl[R]) In() chan any {
	return m.in
}

// Out implements yenstream.Pipeline.
func (m *metricDataStreamImpl[R]) Out() yenstream.NodeOut {
	return m.out
}

// Process implements yenstream.Pipeline.
func (m *metricDataStreamImpl[R]) Process() {
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

func (m *metricDataStreamImpl[R]) flushData(out chan any) {
	m.met.FlushCallback(func(acc any) error {
		out <- acc
		return nil
	})
}

// SetLabel implements yenstream.Pipeline.
func (m *metricDataStreamImpl[R]) SetLabel(label string) {
	m.label = label
}

// Via implements yenstream.Pipeline.
func (m *metricDataStreamImpl[R]) Via(label string, pipe yenstream.Pipeline) yenstream.Pipeline {
	m.ctx.RegisterStream(label, m, pipe)

	return pipe
}

func NewMetricDataStream[R metric.MetricData](
	ctx *yenstream.RunnerContext,
	flushDuration time.Duration,
	met metric.MetricStore[R],
) *metricDataStreamImpl[R] {
	return &metricDataStreamImpl[R]{
		flushDuration: flushDuration,
		ctx:           ctx,
		out:           yenstream.NewNodeOut(ctx),
		in:            make(chan any, 1),
		met:           met,
	}
}
