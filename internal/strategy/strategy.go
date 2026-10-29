// Package strategy holds the competing implementations of the tag search.
package strategy

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"tagbench/internal/shard"
)

// Query: users that have ALL of Tags, optionally restricted to one region.
type Query struct {
	Tags   []int // must be non-empty and deduplicated
	Region int   // -1 = any region
}

type Result struct {
	UserIDs []string
}

type Strategy interface {
	Name() string
	Run(ctx context.Context, q Query) (Result, error)
}

var ErrNotImplemented = errors.New("not implemented yet")

// tagSQL builds the tag-intersection query, optionally restricted to a UUID range.
func tagSQL(q Query, r shard.Range) (string, []any) {
	var sb strings.Builder
	args := make([]any, 0, len(q.Tags)+6)

	sb.WriteString("SELECT u.id FROM users u JOIN user_tags t ON t.user_id = u.id WHERE t.tag_id IN (")
	for i, tag := range q.Tags {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
		args = append(args, tag)
	}
	sb.WriteByte(')')
	if q.Region >= 0 {
		sb.WriteString(" AND u.region = ?")
		args = append(args, q.Region)
	}
	// The range is stated on both sides of the join so each table gets a
	// range scan on its primary key, instead of relying on the optimizer
	// to propagate it through u.id = t.user_id.
	if r.Lo != "" {
		sb.WriteString(" AND u.id >= ? AND t.user_id >= ?")
		args = append(args, r.Lo, r.Lo)
	}
	if r.Hi != "" {
		sb.WriteString(" AND u.id < ? AND t.user_id < ?")
		args = append(args, r.Hi, r.Hi)
	}
	sb.WriteString(" GROUP BY u.id HAVING COUNT(*) = ?")
	args = append(args, len(q.Tags))
	return sb.String(), args
}

func queryIDs(ctx context.Context, db *sql.DB, query string, args []any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
