package stat_replica

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
)

func DumpBytesToFile(path string, data []byte) error {
	// Open the file with append mode, create if not exists, write-only
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Write the data
	encoded := base64.StdEncoding.EncodeToString(data)
	dumpdata := []byte("\n")
	dumpdata = append([]byte(encoded), dumpdata...)
	_, err = file.Write(dumpdata)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	// Ensure data is flushed to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

type ValueHandler func(value []byte)

// ExtractValuesAsBytes reads the file and sends each non-separator line as []byte to the handler
func ExtractValuesAsBytes(path string, handler ValueHandler) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()

		// Trim surrounding spaces
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		decoded, err := base64.StdEncoding.DecodeString(string(trimmed))
		if err != nil {
			return err
		}

		// Pass the raw bytes to handler
		handler(decoded)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	return nil
}
