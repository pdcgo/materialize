package stat_replica_test

import (
	"encoding/json"
	"testing"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/stretchr/testify/assert"
)

func TestBasicCrudParsing(t *testing.T) {
	parser := stat_replica.NewV2Parser(t.Context())
	t.Run("testing insert data", func(t *testing.T) {
		var err error

		fname := "../test_assets/wal_samples/wal_2025-01-08.sample"
		err = stat_replica.ExtractValuesAsBytes(fname, func(value []byte) {
			msg, err := parser.Parse(value)
			assert.Nil(t, err)
			t.Log(msg)
		})

		assert.Nil(t, err)

	})
}

func CombineMapSlices(input map[string][]interface{}, handler func(item map[string]interface{})) {
	if len(input) == 0 {
		return
	}

	// Determine the number of rows by the longest slice
	var maxLen int
	for _, values := range input {
		if len(values) > maxLen {
			maxLen = len(values)
		}
	}

	row := map[string]interface{}{}

	for i := 0; i < maxLen; i++ {

		for key, values := range input {
			if i < len(values) {
				row[key] = values[i]
			}
		}
		handler(row)
	}

}

type TStruct1 struct {
	ID   uint
	Data int
	Name string
}

func TestMapToStruct(t *testing.T) {

	CombineMapSlices(
		map[string][]interface{}{
			"id":   []any{10, float32(10)},
			"data": []any{1, uint(1), float32(1.00)},
			"name": []any{"1", 1, nil},
		},
		func(item map[string]interface{}) {
			raw, _ := json.Marshal(item)
			t.Log(string(raw))

			var d TStruct1
			err := stat_replica.MapToStruct(
				item,
				&d,
			)

			assert.Nil(t, err)
			assert.NotEmpty(t, d)
			assert.Equal(t, int(1), d.Data)
		},
	)

}
