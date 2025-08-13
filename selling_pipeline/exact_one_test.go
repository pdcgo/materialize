package selling_pipeline_test

import (
	"testing"

	"github.com/pdcgo/materialize/selling_pipeline"
	"github.com/pdcgo/materialize/stat_process/db_mock"
	"github.com/pdcgo/materialize/stat_process/exact_one"
	"github.com/pdcgo/materialize/stat_process/models"
	"github.com/pdcgo/materialize/stat_replica"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/yenstream"
	"github.com/stretchr/testify/assert"
)

func TestExactOne(t *testing.T) {
	ctx := t.Context()
	var bdb db_mock.BadgeDBMock

	cdcChan := make(chan *stat_replica.CdcMessage, 1)

	go func() {
		defer close(cdcChan)

		cdcChan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "teams",
				Schema: "public",
			},
			ModType: stat_replica.CdcInsert,
			Data: &models.Team{
				ID:       1,
				Type:     "asdasd",
				Name:     "oldchange",
				TeamCode: "asdasdasd",
			},
		}

		cdcChan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "teams",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.Team{
				ID:       1,
				Type:     "asdasd",
				Name:     "newbackfill",
				TeamCode: "asdasdasd",
			},
		}

		cdcChan <- &stat_replica.CdcMessage{
			SourceMetadata: &stat_replica.SourceMetadata{
				Table:  "teams",
				Schema: "public",
			},
			ModType: stat_replica.CdcBackfill,
			Data: &models.Team{
				ID:       2,
				Type:     "asdasd",
				Name:     "oldchange",
				TeamCode: "asdasdasd",
			},
		}

	}()

	moretest.Suite(t, "testing exact_one",
		moretest.SetupListFunc{
			db_mock.NewBadgeDBMock(&bdb),
		},
		func(t *testing.T) {
			exact := exact_one.NewBadgeExactOne(ctx, bdb.DB)
			yenstream.
				NewRunnerContext(ctx).
				CreatePipeline(func(ctx *yenstream.RunnerContext) yenstream.Pipeline {
					return selling_pipeline.ExactOne(ctx, exact,
						yenstream.NewChannelSource(ctx, cdcChan).
							Via("test", yenstream.NewMap(ctx, func(data any) (any, error) {
								return data, nil
							})),
					)
				})

			t.Run("test tim 1", func(t *testing.T) {
				team := &models.Team{
					ID: 1,
				}

				found, err := exact.GetItemStruct(team)
				assert.Nil(t, err)
				assert.True(t, found)

				assert.Equal(t, "oldchange", team.Name)
			})

			t.Run("test tim 2", func(t *testing.T) {
				team := &models.Team{
					ID: 2,
				}

				found, err := exact.GetItemStruct(team)
				assert.Nil(t, err)
				assert.True(t, found)

				assert.Equal(t, "oldchange", team.Name)
			})
		},
	)

}
