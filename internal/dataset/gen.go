package dataset

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

type Config struct {
	Users   int
	NumTags int // tag ids are 1..NumTags; tag 1 is the most popular
	MinTags int // tags per user, inclusive range
	MaxTags int
	Seed    uint64
}

type User struct {
	ID        string
	Seq       uint32
	Region    uint8
	Age       uint8
	CreatedAt time.Time
	Tags      []int
}

var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// GenChunk generates users with seq in [start, end). The RNG is seeded by
// (Seed, start), so output is identical regardless of worker count.
func GenChunk(cfg Config, start, end int) []User {
	r := rand.New(rand.NewPCG(cfg.Seed, uint64(start)))
	// Zipf: a few tags are very common, most are rare, like real user tags.
	zipf := rand.NewZipf(r, 1.1, 1, uint64(cfg.NumTags-1))

	users := make([]User, 0, end-start)
	seen := make(map[int]struct{}, cfg.MaxTags)
	for seq := start; seq < end; seq++ {
		n := cfg.MinTags + r.IntN(cfg.MaxTags-cfg.MinTags+1)
		clear(seen)
		tags := make([]int, 0, n)
		for attempts := 0; len(tags) < n && attempts < n*20; attempts++ {
			tag := int(zipf.Uint64()) + 1
			if _, dup := seen[tag]; dup {
				continue
			}
			seen[tag] = struct{}{}
			tags = append(tags, tag)
		}
		users = append(users, User{
			ID:        uuidV4(r),
			Seq:       uint32(seq),
			Region:    uint8(r.IntN(20)),
			Age:       uint8(18 + r.IntN(50)),
			CreatedAt: baseTime.Add(time.Duration(r.IntN(365*24*3600)) * time.Second),
			Tags:      tags,
		})
	}
	return users
}

func uuidV4(r *rand.Rand) string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], r.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], r.Uint64())
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// InsertTags fills the tags dictionary table.
func InsertTags(ctx context.Context, db *sql.DB, numTags int) error {
	const batch = 1000
	for lo := 1; lo <= numTags; lo += batch {
		hi := min(lo+batch, numTags+1)
		var sb strings.Builder
		args := make([]any, 0, (hi-lo)*2)
		sb.WriteString("INSERT INTO tags (id, name) VALUES ")
		for id := lo; id < hi; id++ {
			if id > lo {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?)")
			args = append(args, id, fmt.Sprintf("tag_%04d", id))
		}
		if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("insert tags: %w", err)
		}
	}
	return nil
}

// InsertUsers writes users and their user_tags rows as two multi-row INSERTs.
// Returns the number of user_tags rows written.
func InsertUsers(ctx context.Context, db *sql.DB, users []User) (int, error) {
	var sb strings.Builder
	args := make([]any, 0, len(users)*5)
	sb.WriteString("INSERT INTO users (id, seq, region, age, created_at) VALUES ")
	for i, u := range users {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?)")
		args = append(args, u.ID, u.Seq, u.Region, u.Age, u.CreatedAt)
	}
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return 0, fmt.Errorf("insert users: %w", err)
	}

	sb.Reset()
	args = args[:0]
	sb.WriteString("INSERT INTO user_tags (user_id, tag_id) VALUES ")
	for _, u := range users {
		for _, tag := range u.Tags {
			if len(args) > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?)")
			args = append(args, u.ID, tag)
		}
	}
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return 0, fmt.Errorf("insert user_tags: %w", err)
	}
	return len(args) / 2, nil
}
