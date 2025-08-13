package metric

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v3"
)

type MetricData interface {
	Key() string
	Merge(dold interface{}) MetricData
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
	Change(handle func(acc R))
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
	d.Lock()
	datas := d.data
	d.data = map[string]R{}
	d.Unlock()

	var err error
	for _, data := range datas {
		var old R
		old, err = d.getItem(data.Key())
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		newd := data.Merge(old)
		err = d.setItem(newd.Key(), newd.(R))
		if err != nil {
			return err
		}
		data = d.output(newd.(R))
		err = handle(data)
		if err != nil {
			return err
		}
	}

	return nil
}

// Flush implements MetricStore.
func (d *defaultMetricStore[R]) Flush(toChan chan any) { // masih ngebug
	d.Lock()
	datas := d.data
	d.data = map[string]R{}
	d.Unlock()

	for _, data := range datas {
		data = d.output(data)
		toChan <- data
	}

}

// Flush implements MetricStore.
func (d *defaultMetricStore[R]) Change(handle func(acc R)) {
	d.Lock()
	datas := d.data
	d.data = map[string]R{}
	d.Unlock()

	for _, data := range datas {
		handle(data)
	}

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

	// var err error
	var acc R
	// var ok bool

	acc, _ = d.data[key]
	// if !ok {
	// 	acc, err = d.getItem(key)
	// 	if err != nil {
	// 		if !errors.Is(err, badger.ErrKeyNotFound) {
	// 			return err
	// 		}
	// 	}
	// }

	newacc := merger(acc)
	// if err != nil {
	// 	return err
	// }

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
