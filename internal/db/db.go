// Package db opens connections to TiDB (MySQL protocol).
package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

func config(base, dbName string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(base)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.DBName = dbName
	// Interpolate client-side: avoids a server-side prepare round trip per statement.
	cfg.InterpolateParams = true
	cfg.ParseTime = true
	return cfg, nil
}

// EnsureDatabase creates dbName if it does not exist.
func EnsureDatabase(ctx context.Context, base, dbName string) error {
	cfg, err := config(base, "")
	if err != nil {
		return err
	}
	conn, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	return err
}

// Open returns a pool whose size is fixed at maxConns, so benchmark workers
// never wait on connection setup.
func Open(ctx context.Context, base, dbName string, maxConns int) (*sql.DB, error) {
	cfg, err := config(base, dbName)
	if err != nil {
		return nil, err
	}
	conn, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(maxConns)
	conn.SetMaxIdleConns(maxConns)
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping %s: %w", cfg.Addr, err)
	}
	return conn, nil
}
