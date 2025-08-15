package debug_pipeline

import (
	"encoding/json"
	"log"
	"log/slog"
	"os"

	"github.com/pdcgo/shared/yenstream"
)

func Log(ctx *yenstream.RunnerContext) yenstream.Pipeline {
	return yenstream.NewMap(ctx, func(data any) (any, error) {
		raw, _ := json.Marshal(data)
		log.Println(string(raw))
		return data, nil
	})
}

type logFileImpl struct {
	fname string
	ctx   *yenstream.RunnerContext
	label string
	out   yenstream.NodeOut
	in    chan any
}

// In implements yenstream.Pipeline.
func (l *logFileImpl) In() chan any {
	return l.in
}

// Out implements yenstream.Pipeline.
func (l *logFileImpl) Out() yenstream.NodeOut {
	return l.out
}

// Process implements yenstream.Pipeline.
func (l *logFileImpl) Process() {
	out := l.out.C()
	defer close(out)

	file, err := os.OpenFile(l.fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for data := range l.in {
		raw, err := json.Marshal(data)
		if err != nil {
			slog.Error(err.Error(), slog.String("label", l.label))
			continue
		}

		raw = append(raw, []byte("\n")...)
		_, err = file.Write(raw)
		if err != nil {
			slog.Error(err.Error(), slog.String("label", l.label))
			continue
		}

		out <- data

		err = file.Sync()
		if err != nil {
			slog.Error(err.Error(), slog.String("label", l.label))
		}
	}

}

// SetLabel implements yenstream.Pipeline.
func (l *logFileImpl) SetLabel(label string) {
	l.label = label
}

// Via implements yenstream.Pipeline.
func (l *logFileImpl) Via(label string, pipe yenstream.Pipeline) yenstream.Pipeline {
	l.ctx.RegisterStream(label, l, pipe)
	return pipe
}

var _ yenstream.Pipeline = (*logFileImpl)(nil)

func NewLogFile(ctx *yenstream.RunnerContext, fname string) *logFileImpl {

	return &logFileImpl{
		fname: fname,
		ctx:   ctx,
		out:   yenstream.NewNodeOut(ctx),
		in:    make(chan any, 1),
	}
}
