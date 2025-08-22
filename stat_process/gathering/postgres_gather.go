package gathering

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"reflect"
	"time"

	"github.com/pdcgo/materialize/selling_metric"
	"github.com/pdcgo/materialize/stat_process/metric"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type CanFressness interface {
	SetFreshness(n time.Time)
}

type postgresGatherImpl struct {
	db        *gorm.DB
	ctx       context.Context
	metrics   map[string]metric.MetricFlush
	ratelimit chan int
}

// AddMetric implements metric.MetricGather.
func (p *postgresGatherImpl) AddMetric(key string, metric metric.MetricFlush) {
	if p.metrics[key] != nil {
		log.Fatalf("metric %s already exist\n", key)
	}
	p.metrics[key] = metric
}
func (p *postgresGatherImpl) SaveItem(acc any) error {
	// select {
	// case p.ratelimit <- 1:
	// case <-p.ctx.Done():
	// 	return nil
	// }

	// defer func() {
	// 	select {
	// 	case <-p.ratelimit:
	// 	case <-p.ctx.Done():
	// 		return
	// 	}

	// }()

	facc, ok := acc.(CanFressness)
	if !ok {
		name := reflect.TypeOf(acc).Elem().Name()
		return fmt.Errorf("item doesnt implement freshness %s", name)
	}
	facc.SetFreshness(time.Now().Local())
	return p.db.Save(facc).Error
}

func (p *postgresGatherImpl) StartSync() error {

	go func() {
		var err error
		slog.Info("starting sync metric to postgres")

		for {
			time.Sleep(time.Second * 10)
			// slog.Info("running sync", slog.String("gather", "postgres gather"))
			for key, met := range p.metrics {
				err = met.FlushCallback(p.SaveItem)

				if err != nil {
					slog.Error(err.Error(), slog.String("metric", key))
				}
			}

		}
	}()

	return nil
}

// var _ metric.MetricGather = (*postgresGatherImpl)(nil)

func CreateDB() (*gorm.DB, error) {
	var err error
	host := getEnv("STAT_POSTGRES_HOST", "localhost")
	user := getEnv("STAT_POSTGRES_USER", "user")
	pass := getEnv("STAT_POSTGRES_PASSWORD", "password")
	dbname := getEnv("STAT_POSTGRES_DB", "postgres")

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=5432 sslmode=disable TimeZone=Asia/Jakarta",
		host,
		user,
		pass,
		dbname,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return db, err
	}

	err = db.AutoMigrate(
		&metric.DailyShopeepayBalance{},
		&selling_metric.DailyShopMetricData{},
		&selling_metric.DailyTeamMetricData{},
		&selling_metric.DailyBankBalance{},
	)
	if err != nil {
		return db, err
	}

	return db, err
}

func NewPostgresGather(ctx context.Context, db *gorm.DB) *postgresGatherImpl {
	// db, err := CreateDB()
	// if err != nil {
	// 	panic(err)
	// }
	pggat := &postgresGatherImpl{
		db:        db,
		ctx:       ctx,
		metrics:   map[string]metric.MetricFlush{},
		ratelimit: make(chan int, 1),
	}
	return pggat
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
