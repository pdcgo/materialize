package exact_one

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_replica"
)

type ExactHaveKey interface {
	Key() string
}

type ExactlyOnce interface {
	Change(data ExactHaveKey) ChangeExactOne
	Transaction(update bool, handle func(exact ExactlyOnce) error) error
	GetItemStructKey(key string, data ExactHaveKey) (bool, error)
	GetItemStruct(data ExactHaveKey) (bool, error)
	SaveItemStruct(data ...ExactHaveKey) error

	AddItemWithKey(pKey string, cdata *stat_replica.CdcMessage) (bool, error)
	GetItem(key string) (map[string]interface{}, error)
}

type exactOneImpl struct {
	ctx context.Context
	db  *badger.DB
	tx  *badger.Txn
}

// Save implements ExactlyOnce.
func (e *exactOneImpl) Change(data ExactHaveKey) ChangeExactOne {

	var tx *badger.Txn
	var intx bool
	if e.tx != nil {
		intx = true
		tx = e.tx
	} else {
		intx = false
		tx = e.db.NewTransaction(true)
	}
	return NewSaveExactOne(tx, data, intx)
}

// GetItemStructKey implements ExactlyOnce.
func (e *exactOneImpl) GetItemStructKey(key string, data ExactHaveKey) (bool, error) {
	var found bool
	var err error

	err = e.getTx(false, func(exact *exactOneImpl) error {
		item, err := exact.tx.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				found = false
				return nil
			}
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		found = true
		return json.Unmarshal(val, data)
	})

	return found, err
}

// Transaction implements ExactlyOnce.
func (e *exactOneImpl) Transaction(update bool, handle func(exact ExactlyOnce) error) error {
	return e.getTx(update, func(exact *exactOneImpl) error {
		return handle(exact)
	})
}

// GetItemStruct implements ExactlyOnce.
func (e *exactOneImpl) GetItemStruct(data ExactHaveKey) (bool, error) {
	var found bool
	var err error

	err = e.getTx(false, func(exact *exactOneImpl) error {
		item, err := exact.tx.Get([]byte(data.Key()))
		if err != nil {

			if errors.Is(err, badger.ErrKeyNotFound) {
				found = false
				return nil
			}
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		found = true
		return json.Unmarshal(val, data)
	})
	return found, err
}

// SaveItemStruct implements ExactlyOnce.
func (e *exactOneImpl) SaveItemStruct(datas ...ExactHaveKey) error {

	return e.getTx(true, func(exact *exactOneImpl) error {
		var err error

		for _, data := range datas {
			var raw []byte
			raw, err = json.Marshal(data)
			if err != nil {
				return err
			}

			err = exact.tx.Set([]byte(data.Key()), raw)
			if err != nil {
				return err
			}
		}

		err = e.tx.Commit()

		return err
	})

}

func (e *exactOneImpl) getTx(update bool, handle func(exact *exactOneImpl) error) error {
	if e.tx != nil {
		return handle(e)
	}
	if update {
		return e.db.Update(func(txn *badger.Txn) error {
			return handle(&exactOneImpl{
				ctx: e.ctx,
				tx:  txn,
			})
		})
	}
	return e.db.View(func(txn *badger.Txn) error {
		return handle(&exactOneImpl{
			ctx: e.ctx,
			tx:  txn,
		})
	})
}

// GetItem implements ExactlyOnce.
func (e *exactOneImpl) GetItem(key string) (map[string]interface{}, error) {
	var err error
	var result map[string]interface{}
	err = e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return json.Unmarshal(val, &result)
	})

	return result, err
}

// AddItem implements ExactlyOnce.
func (e *exactOneImpl) AddItemWithKey(pKey string, cdata *stat_replica.CdcMessage) (bool, error) {
	var update bool
	var err error

	err = e.db.Update(func(txn *badger.Txn) error {
		key, err := e.extractKey(pKey, cdata)
		if err != nil {
			return err
		}
		var item *badger.Item
		item, err = txn.Get([]byte(key))
		var found bool = true
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				found = false
			} else {
				return err
			}
		}

		var oldata interface{}
		switch cdata.ModType {
		case stat_replica.CdcInsert:
			update = true
			if found {
				oldata, err = e.oldData(cdata.SourceMetadata, item)
				if err != nil {
					return err
				}
				cdata.OldData = oldata
			}

			// cdata.Changes, err = DetectChange(oldata, cdata.Data)
			// if err != nil {
			// 	return err
			// }

		case stat_replica.CdcUpdate:
			update = true
			if found {
				oldata, err = e.oldData(cdata.SourceMetadata, item)
				if err != nil {
					return err
				}
				cdata.OldData = oldata
				// cdata.Changes, err = DetectChange(oldata, cdata.Data)
				// if err != nil {
				// 	return err
				// }
			}
		case stat_replica.CdcBackfill:
			if found {
				return nil
			}
			update = true
		case stat_replica.CdcDelete:
			if !found {
				return nil
			}

			oldata, err = e.oldData(cdata.SourceMetadata, item)
			if err != nil {
				return err
			}
			cdata.OldData = oldata

			err = txn.Delete([]byte(key))
			if err != nil {
				return err
			}
			update = true
			return nil
		}

		raw, err := json.Marshal(cdata.Data)
		if err != nil {
			return err
		}
		return txn.Set([]byte(key), raw)
	})
	return update, err
}

func (e *exactOneImpl) oldData(meta *stat_replica.SourceMetadata, item *badger.Item) (interface{}, error) {
	var err error
	var result interface{}
	result, err = stat_replica.GetCoder(e.ctx, meta.PrefixKey())
	if err != nil {
		if errors.Is(err, stat_replica.ErrCoderNotFound) {
			result = map[string]interface{}{}
		}
	}

	val, err := item.ValueCopy(nil)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(val, &result)
	return result, err
}

func (e *exactOneImpl) extractKey(pKey string, cdata *stat_replica.CdcMessage) (string, error) {
	meta := cdata.SourceMetadata
	prefixKey := fmt.Sprintf("%s/%s/", meta.Schema, meta.Table)

	// extract key
	var key interface{}
	var keyStr string
	var ok bool

	havekey, ok := cdata.Data.(ExactHaveKey)
	if ok {
		return havekey.Key(), nil
	}

	datamap, ok := cdata.Data.(map[string]interface{})
	if !ok {
		return "", errors.New("data exact one key not map[string]interface{}")
	}

	key, ok = datamap[pKey]

	keyStr = ToString(key)
	if keyStr == "" {
		return "", fmt.Errorf("cant parse string pkey %s on source table %s", pKey, cdata.SourceMetadata.Table)
	}

	if !ok {
		return "", fmt.Errorf("cant getting pkey %s", pKey)
	}

	return prefixKey + keyStr, nil
}

func NewBadgeExactOne(ctx context.Context, db *badger.DB) ExactlyOnce {

	return &exactOneImpl{
		ctx: ctx,
		db:  db,
	}
}

func DetectChange(old, new map[string]interface{}) ([]*stat_replica.ChangeItem, error) {
	var err error
	changes := []*stat_replica.ChangeItem{}

	keys := map[string]bool{}
	for key := range old {
		keys[key] = true
	}

	for key := range new {
		keys[key] = true
	}

	for key := range keys {
		oldval := old[key]
		newval := new[key]

		if oldval != newval {
			changes = append(changes, &stat_replica.ChangeItem{
				Field: key,
				Old:   oldval,
				New:   newval,
			})
		}
	}

	return changes, err
}

func ToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case int16:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case uint:
		return fmt.Sprintf("%d", val)
	case uint8:
		return fmt.Sprintf("%d", val)
	case uint16:
		return fmt.Sprintf("%d", val)
	case uint32:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%f", val)
	case float64:
		return fmt.Sprintf("%f", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return ""
	}
}
