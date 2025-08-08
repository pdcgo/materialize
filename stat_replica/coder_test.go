package stat_replica_test

import (
	"testing"

	"github.com/pdcgo/materialize/stat_replica"
	"github.com/stretchr/testify/assert"
)

type DefaultCoder struct {
	DD string
}

func TestCoder(t *testing.T) {
	ctx := stat_replica.ContextWithCoder(t.Context())

	err := stat_replica.RegisterCoder(ctx, "default", &DefaultCoder{})
	assert.Nil(t, err)

	t.Run("testing get coder", func(t *testing.T) {
		newcode, err := stat_replica.GetCoder(ctx, "default")
		assert.Nil(t, err)

		_, ok := newcode.(*DefaultCoder)
		assert.True(t, ok)

		t.Run("testing coder tidak sama referensi", func(t *testing.T) {

			code, err := stat_replica.GetCoder(ctx, "default")
			assert.Nil(t, err)

			dd, ok := code.(*DefaultCoder)
			assert.True(t, ok)

			dd.DD = "asdasdasdasd"
			assert.NotEmpty(t, dd)

			assert.Empty(t, newcode)

		})
	})

	t.Run("testing getting notfound", func(t *testing.T) {
		_, err := stat_replica.GetCoder(ctx, "low")
		assert.NotNil(t, err)
	})

}
