package stat_replica

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type ReplicationHandler func(msg *CdcMessage)

type Replication interface {
	AddHandler(handler ReplicationHandler)
	LogFile(fname string)
	Start() error
}

type replicationImpl struct {
	logfile string
	ctx     context.Context
	conn    *pgconn.PgConn
	handler ReplicationHandler
	parser  Parser
}

// LogFile implements Replication.
func (r *replicationImpl) LogFile(fname string) {
	r.logfile = fname
}

// AddHandler implements Replication.
func (r *replicationImpl) AddHandler(handler ReplicationHandler) {
	r.handler = handler
}

// Start implements Replication.
func (r *replicationImpl) Start() error {
	var err error
	var pluginArguments []string
	// var v2 bool

	pluginArguments = []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", PUB_NAME),
		"messages 'true'",
		"streaming 'true'",
	}
	// v2 = true
	// sysident, err := pglogrepl.IdentifySystem(context.Background(), r.conn)
	// if err != nil {
	// 	return err
	// }
	start := pglogrepl.LSN(0)
	err = pglogrepl.StartReplication(r.ctx, r.conn, SLOT_NAME, start, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return err
	}

	clientXLogPos := start
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(r.ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return err
			}
			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(r.ctx, nextStandbyMessageDeadline)
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			return err
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return err
			}
			// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return err
			}

			if r.logfile != "" {
				DumpBytesToFile(r.logfile, xld.WALData)
			}

			msg, err := r.parser.Parse(xld.WALData)
			if err != nil {
				return err
			}

			r.handler(msg)

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}

}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func NewReplication(ctx context.Context, conn *pgconn.PgConn) Replication {
	parser := NewV2Parser(ctx)
	return &replicationImpl{
		ctx:     ctx,
		conn:    conn,
		handler: func(msg *CdcMessage) {},
		parser:  parser,
	}
}
