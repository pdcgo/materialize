package selling_pipeline_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/metric"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/stretchr/testify/assert"
)

func TestCdcData(t *testing.T) {
	var badgemock db_mock.BadgeDBMock

	moretest.Suite(t, "testing diff account",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&badgemock),
		},
		func(t *testing.T) {
			exact := exact_one.NewBadgeExactOne(t.Context(), badgemock.DB)

			diff := metric.NewDiffAccountCalc(badgemock.DB, exact)

			// {"source_metadata":{"table":"balance_account_histories","schema":"public","database":""},"mod_type":"insert","data":{"account_id":153,"amount":13989896,"at":"2025-08-09T09:01:26Z","created_at":"2025-08-09T09:01:26.775456Z","id":14665,"team_id":79},"old_data":null,"timestamp":1754730266404162}

			t.Run("testing bima sudah diupdate tpi masih 0", func(t *testing.T) {
				raw, err := os.ReadFile("../test_assets/expense/bima_balance_history.json")
				assert.Nil(t, err)
				datas := []*models.BalanceAccountHistory{}
				err = json.Unmarshal(raw, &datas)

				for _, data := range datas {
					cdata := &stat_replica.CdcMessage{
						SourceMetadata: &stat_replica.SourceMetadata{Table: "balance_account_histories", Schema: "public"},
						Data:           data,
						ModType:        stat_replica.CdcBackfill,
					}

					_, err = diff.ProcessCDC(cdata)
					assert.Nil(t, err)
					// t.Error("belum cek selisih")
				}

				assert.Nil(t, err)
			})

			t.Run("test add akun", func(t *testing.T) {
				account := &stat_replica.CdcMessage{
					SourceMetadata: &stat_replica.SourceMetadata{
						Table:  "expense_accounts",
						Schema: "public",
					},
					ModType: stat_replica.CdcInsert,
					Data: &models.ExpenseAccount{
						ID:            153,
						TeamID:        1,
						AccountTypeID: 7,
					},
				}
				_, err := exact.AddItemWithKey("id", account)
				assert.Nil(t, err)
			})

			t.Run("testing insert", func(t *testing.T) {
				_, err := diff.ProcessCDC(&stat_replica.CdcMessage{
					SourceMetadata: &stat_replica.SourceMetadata{
						Table:  "balance_account_histories",
						Schema: "public",
					},
					ModType: stat_replica.CdcInsert,
					Data: &models.BalanceAccountHistory{
						ID:        14665,
						AccountID: 153,
						Amount:    13989896,
						At:        time.Now(),
						CreatedAt: time.Now(),
						TeamID:    79,
					},
				})

				assert.Nil(t, err)
			})

		},
	)

}
