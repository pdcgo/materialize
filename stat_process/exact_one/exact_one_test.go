package exact_one_test

import (
	"testing"
	"time"

	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/stat_db"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/stretchr/testify/assert"
)

func TestExactOne(t *testing.T) {

	var badgedb db_mock.BadgeDBMock

	moretest.Suite(t, "testing exact one",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&badgedb),
		},
		func(t *testing.T) {
			exact := exact_one.NewBadgeExactOne(t.Context(), badgedb.DB)

			insert := stat_replica.CdcMessage{
				SourceMetadata: &stat_replica.SourceMetadata{
					Table:  "test",
					Schema: "public",
				},

				ModType:   stat_replica.CdcInsert,
				Timestamp: time.Now().UnixMicro(),
				Data: map[string]interface{}{
					"id":   1,
					"data": "asdasdada",
				},
			}

			update, err := exact.AddItemWithKey("id", &insert)
			assert.Nil(t, err)
			assert.True(t, update)

			t.Run("testing dengan sideload hrus ada", func(t *testing.T) {
				side := stat_db.NewSideloadDB(badgedb.DB)
				data, err := side.GetWithSchema(&stat_replica.SourceMetadata{
					Table:  "test",
					Schema: "public",
				}, 1)

				assert.Nil(t, err)
				assert.NotEmpty(t, data)

			})

			t.Run("test harusnya sudah update", func(t *testing.T) {
				backfill := stat_replica.CdcMessage{
					SourceMetadata: &stat_replica.SourceMetadata{
						Table:  "test",
						Schema: "public",
					},

					ModType:   stat_replica.CdcBackfill,
					Timestamp: time.Now().UnixMicro(),
					Data: map[string]interface{}{
						"id":   1,
						"data": "backfill",
					},
				}

				update, err := exact.AddItemWithKey("id", &backfill)
				assert.Nil(t, err)
				assert.False(t, update)
			})

			t.Run("test update", func(t *testing.T) {
				updated := stat_replica.CdcMessage{
					SourceMetadata: &stat_replica.SourceMetadata{
						Table:  "test",
						Schema: "public",
					},

					ModType:   stat_replica.CdcUpdate,
					Timestamp: time.Now().UnixMicro(),
					Data: map[string]interface{}{
						"id":   1,
						"data": "update",
					},
				}

				update, err := exact.AddItemWithKey("id", &updated)
				assert.Nil(t, err)
				assert.True(t, update)
				assert.NotEmpty(t, updated.OldData)
				result, err := exact.GetItem("public/test/1")
				assert.Nil(t, err)
				assert.Equal(t, "update", result["data"])

				// raw, _ := json.Marshal(result)
				// log.Println(string(raw))
			})
			t.Run("test delete", func(t *testing.T) {
				deleted := stat_replica.CdcMessage{
					SourceMetadata: &stat_replica.SourceMetadata{
						Table:  "test",
						Schema: "public",
					},

					ModType:   stat_replica.CdcDelete,
					Timestamp: time.Now().UnixMicro(),
					Data: map[string]interface{}{
						"id":   1,
						"data": "delete",
					},
				}

				update, err := exact.AddItemWithKey("id", &deleted)
				assert.Nil(t, err)
				assert.True(t, update)
				assert.NotEmpty(t, deleted.OldData)
				assert.NotEmpty(t, deleted.Data)
			})

		},
	)
}

func TestDetectChange(t *testing.T) {

	t.Run("test not equal", func(t *testing.T) {
		old := map[string]interface{}{
			"field": 12,
		}
		new := map[string]interface{}{}

		changes, _ := exact_one.DetectChange(old, new)
		assert.Len(t, changes, 1)
	})

	t.Run("test equal", func(t *testing.T) {
		old := map[string]interface{}{
			"field": 12,
		}
		new := map[string]interface{}{
			"field": 12,
		}

		changes, _ := exact_one.DetectChange(old, new)
		assert.Len(t, changes, 0)
	})

}
