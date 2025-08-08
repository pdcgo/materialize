package stat_db

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pdcgo/materialize/stat_replica"
)

func NewBadgeDB(dirname string) (*badger.DB, error) {
	opts := badger.DefaultOptions(dirname)
	return badger.Open(opts)
}

type ErrItemNotFound struct {
	Key string `json:"key"`
}

func IsItemNotFound(err error) bool {
	var cerr *ErrItemNotFound
	cek := errors.As(err, &cerr)
	return cek
}

// Error implements error.
func (e *ErrItemNotFound) Error() string {
	return fmt.Sprintf("error key %s not found", e.Key)
}

type SideloadDB interface {
	GetWithSchema(meta *stat_replica.SourceMetadata, id interface{}) (map[string]interface{}, error)
}

type sideloadDBImpl struct {
	db *badger.DB
}

// GetWithSchema implements SideloadDB.
func (s *sideloadDBImpl) GetWithSchema(meta *stat_replica.SourceMetadata, id interface{}) (map[string]interface{}, error) {
	var err error
	result := map[string]interface{}{}
	prefixKey := fmt.Sprintf("%s/%s/", meta.Schema, meta.Table)
	key := ToString(id)

	if key == "" {
		return result, errors.New("sideload key empty")
	}

	bgkey := prefixKey + key

	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(bgkey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &ErrItemNotFound{
					Key: bgkey,
				}
			}
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

func NewSideloadDB(db *badger.DB) SideloadDB {
	return &sideloadDBImpl{
		db: db,
	}
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
