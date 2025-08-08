package metric

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v3"
)

type MetricData interface {
	Key() string
}

type MetricFlush interface {
	Flush(toChan chan any)
	FlushCallback(handle func(acc any) error) error
}

type Preview[R MetricData] interface {
	ToSlice() []R
}

type MetricStore[R MetricData] interface {
	MetricFlush
	Preview[R]

	EmptyAccumulator() R
	Merge(key string, merger func(acc R) R) error
	FlushHandler(handle func(acc R) error) error
}

type defaultMetricStore[R MetricData] struct {
	sync.Mutex
	db     *badger.DB
	data   map[string]R
	cacc   func() R
	output func(r R) R
}

func (d *defaultMetricStore[R]) ToSlice() []R {
	result := make([]R, 0)
	for _, val := range d.data {
		val = d.output(val)
		result = append(result, val)
	}

	return result
}

func (d *defaultMetricStore[R]) EmptyAccumulator() R {
	return d.cacc()
}

func (d *defaultMetricStore[R]) FlushCallback(handle func(acc any) error) error {
	var err error
	for _, data := range d.data {
		data = d.output(data)
		err = handle(data)
		if err != nil {
			return err
		}
	}

	d.Lock()
	defer d.Unlock()
	d.data = map[string]R{}
	return nil
}

// Flush implements MetricStore.
func (d *defaultMetricStore[R]) FlushHandler(handle func(acc R) error) error {
	var err error
	for _, data := range d.data {
		data = d.output(data)
		err = handle(data)
		if err != nil {
			return err
		}
	}

	d.Lock()
	defer d.Unlock()
	d.data = map[string]R{}
	return nil
}

// Flush implements MetricStore.
func (d *defaultMetricStore[R]) Flush(toChan chan any) {
	for _, data := range d.data {
		data = d.output(data)
		toChan <- data
	}

	d.Lock()
	defer d.Unlock()
	d.data = map[string]R{}
}

func (d *defaultMetricStore[R]) getItem(key string) (R, error) {
	acc := d.EmptyAccumulator()

	err := d.db.View(func(txn *badger.Txn) error {
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

func (d *defaultMetricStore[R]) setItem(key string, data R) error {
	var err error
	err = d.db.Update(func(txn *badger.Txn) error {
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

// Merge implements MetricStore.
func (d *defaultMetricStore[R]) Merge(key string, merger func(acc R) R) error {
	d.Lock()
	defer d.Unlock()

	acc, err := d.getItem(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			var d R
			acc = d
		} else {
			return err
		}
	}

	newacc := merger(acc)
	err = d.setItem(key, newacc)
	if err != nil {
		return err
	}

	d.data[key] = newacc
	return nil
}

func NewDefaultMetricStore[R MetricData](db *badger.DB, emptyAcc func() R, output func(R) R) MetricStore[R] {
	if output == nil {
		output = func(r R) R {
			return r
		}
	}
	return &defaultMetricStore[R]{
		db:     db,
		data:   map[string]R{},
		cacc:   emptyAcc,
		output: output,
	}
}
