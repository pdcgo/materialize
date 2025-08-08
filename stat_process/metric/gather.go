package metric

import (
	"encoding/json"
	"fmt"
	"log"
)

type MetricGather interface {
	AddMetric(key string, metric MetricFlush)
}

type DefaultGather struct {
	metrics map[string]MetricFlush
}

func (d *DefaultGather) ToSlice() ([]interface{}, error) {
	var err error
	var datas []interface{}
	outChan := make(chan any, 1)

	go func() {
		defer close(outChan)
		for _, met := range d.metrics {
			met.Flush(outChan)
		}
	}()

	for data := range outChan {
		datas = append(datas, data)
	}

	return datas, err
}

func (d *DefaultGather) Handler(handler func(item interface{}) error) error {
	var err error
	outChan := make(chan any, 1)

	go func() {
		defer close(outChan)
		for _, met := range d.metrics {
			met.Flush(outChan)
		}
	}()

	for data := range outChan {
		err = handler(data)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (d *DefaultGather) Debug() error {
	var err error
	outChan := make(chan any, 1)

	go func() {
		defer close(outChan)
		for _, met := range d.metrics {
			met.Flush(outChan)
		}
	}()

	for data := range outChan {
		var raw []byte
		raw, err = json.Marshal(data)
		if err != nil {
			return err
		}

		log.Println(string(raw))
	}

	return nil
}

// AddMetric implements MetricGather.
func (d *DefaultGather) AddMetric(key string, metric MetricFlush) {

	if d.metrics[key] != nil {
		err := fmt.Errorf("metric exists %s", key)
		panic(err)
	}
	d.metrics[key] = metric

}

// var _ MetricGather = (*DefaultGather)(nil)

func NewDefaultGather() *DefaultGather {
	return &DefaultGather{
		metrics: map[string]MetricFlush{},
	}
}
