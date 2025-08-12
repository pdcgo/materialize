package exact_one_test

import (
	"fmt"
	"testing"

	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/stretchr/testify/assert"
)

type Told struct {
	ID   uint
	Name string
}

// Key implements exact_one.ExactHaveKey.
func (t *Told) Key() string {
	return fmt.Sprintf("%d", t.ID)
}

func TestSaveExactOneBefore(t *testing.T) {
	var badgedb db_mock.BadgeDBMock

	moretest.Suite(t, "testing save",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&badgedb),
		},
		func(t *testing.T) {
			exact := exact_one.NewBadgeExactOne(t.Context(), badgedb.DB)

			new := Told{
				ID:   1,
				Name: "low",
			}

			save := exact.Change(&new)
			err := save.Save().Err()
			assert.Nil(t, err)

			t.Run("test get data", func(t *testing.T) {
				d := Told{
					ID: 1,
				}
				_, err := exact.GetItemStruct(&d)
				assert.Nil(t, err)
				assert.Equal(t, "low", d.Name)

			})

			t.Run("testing saving", func(t *testing.T) {
				new := Told{
					ID:   1,
					Name: "up",
				}
				save := exact.Change(&new)

				before := Told{}
				var found bool
				err = save.
					Before(&found, &before).
					Save().
					Err()

				assert.Nil(t, err)
				assert.Equal(t, "low", before.Name)

				t.Run("test get data", func(t *testing.T) {
					d := Told{
						ID: 1,
					}
					_, err := exact.GetItemStruct(&d)
					assert.Nil(t, err)
					assert.Equal(t, "up", d.Name)

				})

			})

		},
	)
}
