package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pdcgo/shared/yenstream"
)

func createLogStream(ctx *yenstream.RunnerContext, source yenstream.Source) yenstream.Pipeline {
	os.MkdirAll("./streamdata/logstream", os.ModeDir)

	fpath := "./streamdata/logstream/"

	return source.
		Via("stream_file", yenstream.NewMap(ctx, func(data any) (any, error) {
			fname := fpath + time.Now().Format("2006-01-02") + ".log"
			// Open the file with append mode, create if not exists, write-only
			file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return data, fmt.Errorf("failed to open file: %w", err)
			}
			defer file.Close()

			// Write the data
			raw, err := json.Marshal(data)
			if err != nil {
				return data, err
			}
			raw = append(raw, []byte("\n")...)
			_, err = file.Write(raw)
			if err != nil {
				return data, fmt.Errorf("failed to write to file: %w", err)
			}

			// Ensure data is flushed to disk
			if err := file.Sync(); err != nil {
				return data, fmt.Errorf("failed to sync file: %w", err)
			}

			return data, nil
		}))
}
