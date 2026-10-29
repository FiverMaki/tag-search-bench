package strategy

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"tagbench/internal/shard"
)

// Sharded splits the UUID space into Shards ranges and queries them with a
// pool of Workers goroutines (scatter-gather).
type Sharded struct {
	DB      *sql.DB
	Shards  int
	Workers int
}

func (s *Sharded) Name() string {
	return fmt.Sprintf("sharded(shards=%d,workers=%d)", s.Shards, s.Workers)
}

func (s *Sharded) Run(ctx context.Context, q Query) (Result, error) {
	ranges, err := shard.Ranges(s.Shards)
	if err != nil {
		return Result{}, err
	}

	// The first failing shard cancels the others: a partial result is useless.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan shard.Range)
	// Buffered to len(ranges) so a worker never blocks on send, even after
	// the collector has stopped reading.
	results := make(chan []string, len(ranges))

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for range min(s.Workers, len(ranges)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range jobs {
				query, args := tagSQL(q, r)
				ids, err := queryIDs(ctx, s.DB, query, args)
				if err != nil {
					errOnce.Do(func() { firstErr = err; cancel() })
					return
				}
				results <- ids
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, r := range ranges {
			select {
			case jobs <- r:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(results)
	if firstErr != nil {
		return Result{}, firstErr
	}

	// Ranges are disjoint, so the merge is a plain concatenation, no dedup.
	parts := make([][]string, 0, len(ranges))
	total := 0
	for ids := range results {
		parts = append(parts, ids)
		total += len(ids)
	}
	merged := make([]string, 0, total)
	for _, ids := range parts {
		merged = append(merged, ids...)
	}
	return Result{UserIDs: merged}, nil
}
