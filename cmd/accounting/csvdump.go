package main

import (
	"encoding/csv"
	"os"
)

type CsvItem interface {
	CsvHeaders() []string
	CsvData() ([]string, error)
}

func SaveCsv[T CsvItem](fname string, datas []T) error {
	var err error

	file, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for i, data := range datas {
		if i == 0 {
			writer.Write(data.CsvHeaders())
		}
		raw, err := data.CsvData()
		if err != nil {
			return err
		}
		writer.Write(raw)
	}

	return err
}
