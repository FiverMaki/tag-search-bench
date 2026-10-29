package strategy

import (
	"context"
	"database/sql"

	"tagbench/internal/shard"
)

// Baseline sends one query and lets TiDB do everything.
type Baseline struct {
	DB *sql.DB
}

func (b *Baseline) Name() string { return "baseline" }

func (b *Baseline) Run(ctx context.Context, q Query) (Result, error) {
	query, args := tagSQL(q, shard.Range{})
	ids, err := queryIDs(ctx, b.DB, query, args)
	return Result{UserIDs: ids}, err
}
