package debug_pipeline

import (
	"encoding/json"
	"log"

	"github.com/pdcgo/shared/yenstream"
)

func Log(ctx *yenstream.RunnerContext) yenstream.Pipeline {
	return yenstream.NewMap(ctx, func(data any) (any, error) {
		raw, _ := json.Marshal(data)
		log.Println(string(raw))
		return data, nil
	})
}
