package stat_replica

import "fmt"

// {
//   "sourceMetadata": {
//     "table": "users",
//     "schema": "public",
//     "database": "appdb"
//   },
//   "modType": "UPDATE",
//   "data": {
//     "id": 101,
//     "name": "Alice Smith"
//   },
//   "oldData": {
//     "name": "Alice"
//   },
//   "modTimestamp": "2025-07-31T03:19:10.456789Z",
//   "transactionId": "84937285"
// }

type SourceMetadata struct {
	Table    string `json:"table"`
	Schema   string `json:"schema"`
	Database string `json:"database"`
}

func (meta *SourceMetadata) PrefixKey() string {
	return fmt.Sprintf("%s/%s/", meta.Schema, meta.Table)
}

type ModificationType string

const (
	CdcUpdate   ModificationType = "update"
	CdcDelete   ModificationType = "delete"
	CdcInsert   ModificationType = "insert"
	CdcBackfill ModificationType = "backfill"
)

type ChangeItem struct {
	Field string      `json:"field"`
	Old   interface{} `json:"old"`
	New   interface{} `json:"new"`
}

type CdcMessage struct {
	SourceMetadata *SourceMetadata  `json:"source_metadata"`
	ModType        ModificationType `json:"mod_type"`
	Data           interface{}      `json:"data"`
	OldData        interface{}      `json:"old_data"`
	// Changes        []*ChangeItem    `json:"changes"`
	Timestamp int64 `json:"timestamp"`
}
