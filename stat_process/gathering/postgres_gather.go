package gathering

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/pdcgo/materialize/stat_process/metric"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type postgresGatherImpl struct {
	ctx     context.Context
	metrics map[string]metric.MetricFlush
}

// AddMetric implements metric.MetricGather.
func (p *postgresGatherImpl) AddMetric(key string, metric metric.MetricFlush) {
	if p.metrics[key] != nil {
		log.Fatalf("metric %s already exist\n", key)
	}
	p.metrics[key] = metric
}

func (p *postgresGatherImpl) StartSync() error {
	dsn := "host=localhost user=user password=password dbname=postgres port=5432 sslmode=disable TimeZone=Asia/Jakarta"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	err = db.AutoMigrate(&metric.DailyShopeepayBalance{})
	if err != nil {
		return err
	}

	go func() {
		slog.Info("starting sync metric to postgres")

		for {
			// slog.Info("running sync", slog.String("gather", "postgres gather"))
			for key, met := range p.metrics {
				err = met.FlushCallback(func(acc any) error {
					return db.Save(acc).Error
				})

				if err != nil {
					slog.Error(err.Error(), slog.String("metric", key))
				}
			}
			time.Sleep(time.Second * 5)
		}
	}()

	return nil
}

// var _ metric.MetricGather = (*postgresGatherImpl)(nil)

func NewPostgresGather(ctx context.Context) *postgresGatherImpl {
	return &postgresGatherImpl{
		ctx:     ctx,
		metrics: map[string]metric.MetricFlush{},
	}
}
