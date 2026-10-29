package store

import (
	"context"
	"database/sql"
	"time"
)

// HostSocket is an operator-approved Host Unix socket that an environment's containers reach at a port
// of the Host virtual address; EnvironmentID empty: every environment of this Host.
type HostSocket struct {
	EnvironmentID string
	Port          uint16
	Path          string
	Revision      uint64
}

// nullable is the environment column's value: NULL for every environment.
func nullable(environment string) any {
	if environment == "" {
		return nil
	}
	return environment
}

// SetHostSocket approves path at port for environment (empty: every environment); a port that has one
// already gets the new path.
func (store *Store) SetHostSocket(ctx context.Context, environment string, port uint16, path string) (HostSocket, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return HostSocket{}, err
	}
	defer tx.Rollback()
	var had string
	var revision uint64
	err = tx.QueryRowContext(ctx, `SELECT path,revision FROM host_sockets WHERE IFNULL(environment_id,'')=? AND port=?`, environment, port).Scan(&had, &revision)
	switch {
	case err == sql.ErrNoRows:
		revision = 1
		_, err = tx.ExecContext(ctx, `INSERT INTO host_sockets(environment_id,port,path,revision,updated_at) VALUES(?,?,?,1,?)`, nullable(environment), port, path, time.Now().UnixNano())
	case err == nil && had != path:
		revision++
		_, err = tx.ExecContext(ctx, `UPDATE host_sockets SET path=?,revision=?,updated_at=? WHERE IFNULL(environment_id,'')=? AND port=?`, path, revision, time.Now().UnixNano(), environment, port)
	}
	if err != nil {
		return HostSocket{}, err
	}
	if err := tx.Commit(); err != nil {
		return HostSocket{}, err
	}
	return HostSocket{EnvironmentID: environment, Port: port, Path: path, Revision: revision}, nil
}

// RemoveHostSocket removes the socket at port of environment (empty: of every environment);
// sql.ErrNoRows when there is none.
func (store *Store) RemoveHostSocket(ctx context.Context, environment string, port uint16) error {
	result, err := store.db.ExecContext(ctx, `DELETE FROM host_sockets WHERE IFNULL(environment_id,'')=? AND port=?`, environment, port)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// ListHostSockets reads the approved sockets: all of them when environment is empty, else those of
// environment and those of every environment.
func (store *Store) ListHostSockets(ctx context.Context, environment string) ([]HostSocket, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT IFNULL(environment_id,''),port,path,revision FROM host_sockets WHERE ?='' OR environment_id IS NULL OR environment_id=? ORDER BY IFNULL(environment_id,''),port`, environment, environment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []HostSocket
	for rows.Next() {
		var value HostSocket
		if err := rows.Scan(&value.EnvironmentID, &value.Port, &value.Path, &value.Revision); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

// HostSocketsOf is what environment reaches: every environment's sockets, and its own in their place at the
// same ports.
func HostSocketsOf(all []HostSocket, environment string) []HostSocket {
	var result []HostSocket
	for _, socket := range all {
		if socket.EnvironmentID != "" {
			continue
		}
		result = append(result, socket)
	}
	for _, socket := range all {
		if socket.EnvironmentID != environment || environment == "" {
			continue
		}
		replaced := false
		for index := range result {
			if result[index].Port == socket.Port {
				result[index], replaced = socket, true
			}
		}
		if !replaced {
			result = append(result, socket)
		}
	}
	return result
}
