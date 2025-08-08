package stat_replica

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type Parser interface {
	Parse(walData []byte) (*CdcMessage, error)
}

type v2ParseImpl struct {
	ctx       context.Context
	typemap   *pgtype.Map
	inStream  *bool
	relations map[uint32]*pglogrepl.RelationMessageV2
}

func (v *v2ParseImpl) convertToCoder(dataMap map[string]interface{}, cdc *CdcMessage) error {
	code, err := GetCoder(v.ctx, cdc.SourceMetadata.PrefixKey())
	if err != nil {
		if !errors.Is(err, ErrCoderNotFound) {
			return err
		} else {
			cdc.Data = dataMap
			return nil
		}
	}
	cdc.Data = code

	err = MapToStruct(dataMap, code)
	if err != nil {
		meta := cdc.SourceMetadata
		slog.Error(
			err.Error(),
			slog.String("table", meta.Table),
			slog.String("schema", meta.Schema),
			slog.String("database", meta.Database),
		)
		return err
	}

	return nil
}

// Parse implements Parser.
func (v *v2ParseImpl) Parse(walData []byte) (*CdcMessage, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *v.inStream)
	if err != nil {
		return nil, fmt.Errorf("Parse Logical Message Failed %s", err.Error())
	}
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		v.relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, ok := v.relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}

		cdc := CdcMessage{
			SourceMetadata: &SourceMetadata{
				Table:    rel.RelationName,
				Schema:   rel.Namespace,
				Database: "",
			},
			ModType:   CdcInsert,
			Data:      map[string]interface{}{},
			Timestamp: time.Now().UnixMicro(),
		}

		dataMap := map[string]interface{}{}

		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				dataMap[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(v.typemap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				dataMap[colName] = val
			}
		}

		err = v.convertToCoder(dataMap, &cdc)
		return &cdc, err

	case *pglogrepl.UpdateMessageV2:

		rel, ok := v.relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}

		cdc := CdcMessage{
			SourceMetadata: &SourceMetadata{
				Table:    rel.RelationName,
				Schema:   rel.Namespace,
				Database: "",
			},
			ModType:   CdcUpdate,
			Data:      map[string]interface{}{},
			Timestamp: time.Now().UnixMicro(),
		}

		dataMap := map[string]interface{}{}

		for idx, col := range logicalMsg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				dataMap[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(v.typemap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				dataMap[colName] = val
			}
		}
		err = v.convertToCoder(dataMap, &cdc)
		return &cdc, err

	case *pglogrepl.DeleteMessageV2:
		rel, ok := v.relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}

		cdc := CdcMessage{
			SourceMetadata: &SourceMetadata{
				Table:    rel.RelationName,
				Schema:   rel.Namespace,
				Database: "",
			},
			ModType:   CdcDelete,
			Data:      map[string]interface{}{},
			Timestamp: time.Now().UnixMicro(),
		}

		dataMap := map[string]interface{}{}

		for idx, col := range logicalMsg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				dataMap[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(v.typemap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				dataMap[colName] = val
			}
		}

		err = v.convertToCoder(dataMap, &cdc)
		return &cdc, err

		// ...
	case *pglogrepl.TruncateMessageV2:
		// log.Printf("truncate for xid %d\n", logicalMsg.Xid)
		// ...

	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessageV2:
		// log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	case *pglogrepl.StreamStartMessageV2:
		*v.inStream = true
		// log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		*v.inStream = false
		// log.Printf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		// log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		// log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		return nil, fmt.Errorf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}

	return nil, nil
}

func NewV2Parser(ctx context.Context) Parser {
	typeMap := pgtype.NewMap()
	inStream := false
	return &v2ParseImpl{
		ctx:       ctx,
		relations: map[uint32]*pglogrepl.RelationMessageV2{},
		typemap:   typeMap,
		inStream:  &inStream,
	}
}

var PG_NUMERIC reflect.Type = reflect.TypeOf(pgtype.Numeric{})

func MapToStruct(input map[string]interface{}, output interface{}) error {
	v := reflect.ValueOf(output)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("MapToStruct output must be a pointer to a struct")
	}

	v = v.Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		structField := t.Field(i)
		fieldName := toSnakeCase(structField.Name)
		field := v.Field(i)

		// log.Println(fieldName, t.NumField())
		// if fieldName == "fund_at" {
		// 	panic("asdasd")
		// }

		if !field.CanSet() {
			continue
		}

		mapVal, ok := input[fieldName]
		if !ok || mapVal == nil {
			continue
		}

		mapValValue := reflect.ValueOf(mapVal)

		if mapValValue.Type().ConvertibleTo(structField.Type) {
			field.Set(mapValValue.Convert(structField.Type))
		} else {
			switch mapValValue.Type() {
			case PG_NUMERIC:
				pgnum := mapValValue.Interface().(pgtype.Numeric)
				pgfloat, err := pgnum.Float64Value()
				if err != nil {
					return err
				}

				field.Set(reflect.ValueOf(pgfloat.Float64))
				continue
			}

			name := mapValValue.Type().Name()
			dstname := structField.Type.Name()
			err := fmt.Errorf("fieldname %s %s cannot convertible to %s", fieldName, name, dstname)
			return err
		}
	}

	return nil
}

func toSnakeCase(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 5) // preallocate a bit more space

	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 && (unicode.IsLower(rune(s[i-1])) || (i+1 < len(s) && unicode.IsLower(rune(s[i+1])))) {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}

	return b.String()
}
