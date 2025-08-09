package stat_replica

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type InitReplica struct {
	cfg  *ReplicationConfig
	ctx  context.Context
	conn *pgconn.PgConn
	err  error
}

func (i *InitReplica) Initialize(slotTemporary bool) *InitReplica {
	slog.Info("check publication")
	exist, err := i.publicationExist(i.cfg.PublicationName)
	if err != nil {
		return i.setErr(err)
	}
	if !exist {
		slog.Info("create publication", slog.String("pubname", i.cfg.PublicationName))
		err = i.createPublication(i.cfg.PublicationName)
		if err != nil {
			return i.setErr(err)
		}

	}

	slog.Info("check slot")
	exist, err = i.slotExist(i.cfg.SlotName)
	if err != nil {
		return i.setErr(err)
	}

	if !exist {
		slog.Info("create slot", slog.String("slotname", i.cfg.SlotName))
		err = i.createSlot(i.cfg.SlotName, slotTemporary)
		if err != nil {
			return i.setErr(err)
		}

	}

	return i
}

func (i *InitReplica) Clear() *InitReplica {
	slog.Info("delete publication")
	exist, err := i.publicationExist(i.cfg.PublicationName)
	if err != nil {
		return i.setErr(err)
	}
	if exist {
		slog.Info("delete publication", slog.String("pubname", i.cfg.PublicationName))
		err = i.deletePublication(i.cfg.PublicationName)
		if err != nil {
			return i.setErr(err)
		}

	}

	slog.Info("delete slot")
	exist, err = i.slotExist(i.cfg.SlotName)
	if err != nil {
		return i.setErr(err)
	}

	if exist {
		slog.Info("delete slot", slog.String("slotname", i.cfg.SlotName))
		err = i.deleteSlot(i.cfg.SlotName)
		if err != nil {
			return i.setErr(err)
		}

	}

	return i
}

func (i *InitReplica) deleteSlot(name string) error {
	query := fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", name)
	res := i.conn.Exec(i.ctx, query)
	data, err := res.ReadAll()
	if err != nil {
		return err
	}
	if len(data) > 0 && data[0].Err != nil {
		return data[0].Err
	}
	return nil
}

func (i *InitReplica) createSlot(name string, temporary bool) error {
	_, err := pglogrepl.CreateReplicationSlot(i.ctx, i.conn, name, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	// query := fmt.Sprintf("SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput')", name)
	// res := i.conn.Exec(i.ctx, query)
	// data, err := res.ReadAll()
	// if err != nil {
	// 	return err
	// }
	// if len(data) > 0 && data[0].Err != nil {
	// 	return data[0].Err
	// }
	// return nil
	return err
}

func (i *InitReplica) slotExist(name string) (bool, error) {
	query := fmt.Sprintf("SELECT * FROM pg_replication_slots WHERE slot_name = '%s'", name)
	res := i.conn.Exec(i.ctx, query)
	data, err := res.ReadAll()
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, nil
	}
	if data[0].Err != nil {
		return false, data[0].Err
	}
	if len(data[0].Rows) >= 1 {
		return true, nil
	}
	return false, nil
}

func (i *InitReplica) deletePublication(name string) error {
	query := fmt.Sprintf("DROP PUBLICATION %s", name)
	res := i.conn.Exec(i.ctx, query)
	data, err := res.ReadAll()
	if err != nil {
		return err
	}
	if len(data) > 0 && data[0].Err != nil {
		return data[0].Err
	}
	return nil
}

func (i *InitReplica) createPublication(name string) error {
	query := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", name)
	res := i.conn.Exec(i.ctx, query)
	data, err := res.ReadAll()
	if err != nil {
		return err
	}
	if len(data) > 0 && data[0].Err != nil {
		return data[0].Err
	}
	return nil
}

func (i *InitReplica) publicationExist(name string) (bool, error) {
	query := fmt.Sprintf("SELECT * FROM pg_publication where pubname = '%s'", name)
	// query := "SELECT * FROM pg_publication"

	res := i.conn.Exec(i.ctx, query)
	data, err := res.ReadAll()
	if err != nil {
		return false, err
	}

	err = data[0].Err
	if err != nil {
		return false, err
	}
	values := data[0].Rows
	if len(values) >= 1 {
		return true, nil
	}

	return false, err
}

func (i *InitReplica) setErr(err error) *InitReplica {
	if i.err != nil {
		return i
	}
	if err != nil {
		i.err = err
	}
	return i
}

func (i *InitReplica) Err() error {
	return i.err
}

func NewInitReplica(ctx context.Context, conn *pgconn.PgConn, cfg *ReplicationConfig) *InitReplica {
	return &InitReplica{
		cfg:  cfg,
		ctx:  ctx,
		conn: conn,
	}
}
