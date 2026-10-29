package strategy

import (
	"context"
	"strings"
	"sync"

	"tagbench/internal/shard"
)

// runRanges calls fn for every range on a pool of workers. fn receives the
// range index, so callers can write results into a pre-sized slice without
// locking. The first error cancels the remaining calls and is returned.
func runRanges(ctx context.Context, ranges []shard.Range, workers int,
	fn func(ctx context.Context, i int, r shard.Range) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for range min(workers, len(ranges)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := fn(ctx, i, ranges[i]); err != nil {
					errOnce.Do(func() { firstErr = err; cancel() })
					return
				}
			}
		}()
	}

feed:
	for i := range ranges {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// rangeWhere returns " WHERE ..." restricting every column in cols to r,
// or "" when r is unbounded.
func rangeWhere(r shard.Range, cols ...string) (string, []any) {
	var conds []string
	var args []any
	for _, c := range cols {
		if r.Lo != "" {
			conds = append(conds, c+" >= ?")
			args = append(args, r.Lo)
		}
		if r.Hi != "" {
			conds = append(conds, c+" < ?")
			args = append(args, r.Hi)
		}
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}
