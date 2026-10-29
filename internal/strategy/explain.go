package strategy

import (
	"context"
	"database/sql"
	"strings"
	"text/tabwriter"

	"tagbench/internal/shard"
)

// ExplainAnalyze runs EXPLAIN ANALYZE on the query for range r and returns
// the plan as an aligned text table.
func ExplainAnalyze(ctx context.Context, db *sql.DB, q Query, r shard.Range) (string, error) {
	query, args := tagSQL(q, r)
	rows, err := db.QueryContext(ctx, "EXPLAIN ANALYZE "+query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
	w.Write([]byte(strings.Join(cols, "\t") + "\n"))
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		cells := make([]string, len(vals))
		for i, v := range vals {
			cells[i] = v.String
		}
		w.Write([]byte(strings.Join(cells, "\t") + "\n"))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	w.Flush()
	return sb.String(), nil
}
