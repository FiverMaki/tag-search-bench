// bench runs a search strategy repeatedly and reports latency.
//
//	bench -strategy baseline -tags 3,17,120
//	bench -strategy sharded -shards 16 -workers 16 -verify
//	bench -sweep 8,16,32,64,128,256,512 -workers 16 -out results/sweep.csv
package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"tagbench/internal/db"
	"tagbench/internal/shard"
	"tagbench/internal/strategy"
)

func main() {
	dsn := flag.String("dsn", "root@tcp(127.0.0.1:4000)/", "TiDB DSN, database part is ignored")
	dbName := flag.String("db", "tagbench", "database name")
	name := flag.String("strategy", "baseline", "baseline | sharded | bitmap")
	tagsFlag := flag.String("tags", "3,17,120", "comma-separated tag ids; users must have all of them")
	region := flag.Int("region", -1, "restrict to region 0..19, -1 = any")
	shards := flag.Int("shards", 16, "sharded: number of UUID ranges")
	workers := flag.Int("workers", 16, "sharded: concurrent queries")
	runs := flag.Int("runs", 5, "measured runs")
	warmup := flag.Int("warmup", 1, "unmeasured warm-up runs")
	verify := flag.Bool("verify", false, "check the result equals baseline's")
	sweep := flag.String("sweep", "", "comma-separated shard counts; runs sharded for each and writes CSV")
	out := flag.String("out", "", "CSV output path for -sweep (default stdout)")
	timeout := flag.Duration("timeout", 10*time.Minute, "overall timeout")
	explain := flag.Bool("explain", false, "print EXPLAIN ANALYZE of the baseline query and exit")
	flag.Parse()

	tags, err := parseInts(*tagsFlag)
	if err != nil || len(tags) == 0 {
		log.Fatalf("bad -tags %q: %v", *tagsFlag, err)
	}
	slices.Sort(tags)
	q := strategy.Query{Tags: slices.Compact(tags), Region: *region}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	conn, err := db.Open(ctx, *dsn, *dbName, *workers+2)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if *explain {
		plan, err := strategy.ExplainAnalyze(ctx, conn, q, shard.Range{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(plan)
		return
	}

	var want []string
	if *verify {
		res, err := (&strategy.Baseline{DB: conn}).Run(ctx, q)
		if err != nil {
			log.Fatalf("verify: baseline: %v", err)
		}
		want = sorted(res.UserIDs)
		log.Printf("verify: baseline returned %d users", len(want))
	}

	if *sweep != "" {
		counts, err := parseInts(*sweep)
		if err != nil {
			log.Fatalf("bad -sweep: %v", err)
		}
		runSweep(ctx, conn, q, counts, *workers, *warmup, *runs, want, *out)
		return
	}

	s, err := build(ctx, *name, conn, *shards, *workers)
	if err != nil {
		log.Fatal(err)
	}
	durs, count := measure(ctx, s, q, *warmup, *runs, want)
	fmt.Printf("%s  tags=%v region=%d  result=%d users\n", s.Name(), q.Tags, q.Region, count)
	fmt.Printf("  runs=%d  min=%s  p50=%s  max=%s\n", len(durs),
		durs[0].Round(time.Millisecond), durs[len(durs)/2].Round(time.Millisecond), durs[len(durs)-1].Round(time.Millisecond))

	// Run() includes formatting every matched id; time the AND by itself too.
	// It is far below the Windows clock resolution, so average over many calls.
	if b, ok := s.(*strategy.Bitmap); ok {
		const iters = 100
		and := make([]time.Duration, 0, *runs)
		for range *runs {
			start := time.Now()
			for range iters {
				b.Match(q)
			}
			and = append(and, time.Since(start)/iters)
		}
		slices.Sort(and)
		fmt.Printf("  bitmap AND only: min=%s  p50=%s  max=%s\n", and[0], and[len(and)/2], and[len(and)-1])
	}
}

func build(ctx context.Context, name string, conn *sql.DB, shards, workers int) (strategy.Strategy, error) {
	switch name {
	case "baseline":
		return &strategy.Baseline{DB: conn}, nil
	case "sharded":
		return &strategy.Sharded{DB: conn, Shards: shards, Workers: workers}, nil
	case "bitmap":
		b := &strategy.Bitmap{DB: conn, Workers: workers}
		start := time.Now()
		if err := b.Build(ctx); err != nil {
			return nil, fmt.Errorf("bitmap build: %w", err)
		}
		log.Printf("bitmap index built in %s: %s", time.Since(start).Round(time.Millisecond), b.Stats())
		return b, nil
	}
	return nil, fmt.Errorf("unknown strategy %q", name)
}

// measure returns sorted run durations and the result size.
func measure(ctx context.Context, s strategy.Strategy, q strategy.Query, warmup, runs int, want []string) ([]time.Duration, int) {
	var count int
	durs := make([]time.Duration, 0, runs)
	for i := range warmup + runs {
		start := time.Now()
		res, err := s.Run(ctx, q)
		d := time.Since(start)
		if err != nil {
			log.Fatalf("%s: %v", s.Name(), err)
		}
		if want != nil && i == 0 && !slices.Equal(sorted(res.UserIDs), want) {
			log.Fatalf("%s: result differs from baseline (%d vs %d users)", s.Name(), len(res.UserIDs), len(want))
		}
		count = len(res.UserIDs)
		if i >= warmup {
			durs = append(durs, d)
		}
	}
	slices.Sort(durs)
	return durs, count
}

func runSweep(ctx context.Context, conn *sql.DB, q strategy.Query, counts []int, workers, warmup, runs int, want []string, out string) {
	f := os.Stdout
	if out != "" {
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			log.Fatal(err)
		}
		var err error
		if f, err = os.Create(out); err != nil {
			log.Fatal(err)
		}
		defer f.Close()
	}
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"shards", "workers", "min_ms", "p50_ms", "max_ms", "result"})

	for _, n := range counts {
		s := &strategy.Sharded{DB: conn, Shards: n, Workers: workers}
		durs, count := measure(ctx, s, q, warmup, runs, want)
		ms := func(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }
		w.Write([]string{strconv.Itoa(n), strconv.Itoa(workers),
			ms(durs[0]), ms(durs[len(durs)/2]), ms(durs[len(durs)-1]), strconv.Itoa(count)})
		w.Flush()
		log.Printf("shards=%d p50=%s", n, durs[len(durs)/2].Round(time.Millisecond))
	}
}

func parseInts(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func sorted(ids []string) []string {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}
