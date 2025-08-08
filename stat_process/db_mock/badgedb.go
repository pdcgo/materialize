package db_mock

import (
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/stretchr/testify/assert"
)

type BadgeDBMock struct {
	DB *badger.DB
}

func NewBadgeDBMock(db *BadgeDBMock) moretest.SetupFunc {
	return func(t *testing.T) func() error {
		id := uuid.New()
		dirname := fmt.Sprintf("/tmp/badge_mock/%s", id.String())
		var err error
		opts := badger.DefaultOptions(dirname)
		db.DB, err = badger.Open(opts)
		assert.Nil(t, err)

		return func() error {
			db.DB.Close()
			return os.RemoveAll(dirname)
		}
	}

}
