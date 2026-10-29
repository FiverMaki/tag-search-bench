// gen creates the schema and loads reproducible users / user_tags data.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"tagbench/internal/dataset"
	"tagbench/internal/db"
)

func main() {
	dsn := flag.String("dsn", "root@tcp(127.0.0.1:4000)/", "TiDB DSN, database part is ignored")
	dbName := flag.String("db", "tagbench", "database name")
	users := flag.Int("users", 300_000, "number of users (the original case: 3000000)")
	numTags := flag.Int("tags", 2000, "tag vocabulary size")
	minTags := flag.Int("min-tags", 4, "min tags per user")
	maxTags := flag.Int("max-tags", 12, "max tags per user (avg ~8 => 24M rows for 3M users)")
	batch := flag.Int("batch", 1000, "users per INSERT")
	workers := flag.Int("workers", 8, "concurrent loaders")
	seed := flag.Uint64("seed", 42, "random seed")
	drop := flag.Bool("drop", false, "drop existing tables first (required when reloading)")
	flag.Parse()

	cfg := dataset.Config{Users: *users, NumTags: *numTags, MinTags: *minTags, MaxTags: *maxTags, Seed: *seed}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := db.EnsureDatabase(ctx, *dsn, *dbName); err != nil {
		log.Fatalf("create database: %v", err)
	}
	conn, err := db.Open(ctx, *dsn, *dbName, *workers+1)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if *drop {
		execAll(ctx, conn, dataset.Drop)
	}
	execAll(ctx, conn, dataset.Schema)
	if err := dataset.InsertTags(ctx, conn, cfg.NumTags); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	var doneUsers, doneTags atomic.Int64
	chunks := make(chan int)
	go func() {
		defer close(chunks)
		for lo := 0; lo < cfg.Users; lo += *batch {
			select {
			case chunks <- lo:
			case <-ctx.Done():
				return
			}
		}
	}()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for lo := range chunks {
				hi := min(lo+*batch, cfg.Users)
				n, err := dataset.InsertUsers(ctx, conn, dataset.GenChunk(cfg, lo, hi))
				if err != nil {
					errOnce.Do(func() { firstErr = err; cancel() })
					return
				}
				doneUsers.Add(int64(hi - lo))
				doneTags.Add(int64(n))
			}
		}()
	}

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				u := doneUsers.Load()
				log.Printf("users %d/%d (%.0f%%), user_tags %d, %.0f users/s",
					u, cfg.Users, float64(u)*100/float64(cfg.Users), doneTags.Load(),
					float64(u)/time.Since(start).Seconds())
			case <-stop:
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	if firstErr != nil {
		log.Fatalf("load failed (rerun with -drop): %v", firstErr)
	}
	log.Printf("loaded %d users, %d user_tags in %s", doneUsers.Load(), doneTags.Load(), time.Since(start).Round(time.Second))

	// Fresh statistics, otherwise the optimizer plans with empty-table estimates.
	log.Print("analyzing tables...")
	execAll(ctx, conn, dataset.Analyze)
	log.Print("done")
}

func execAll(ctx context.Context, conn *sql.DB, stmts []string) {
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			log.Fatalf("%s: %v", s, err)
		}
	}
}
