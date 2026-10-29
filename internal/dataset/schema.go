// Package dataset defines the schema and generates reproducible test data.
package dataset

// Schema is applied in order by cmd/gen.
//
// users.id is a random UUID v4 string, matching the original production table.
// users.seq is a dense 0..N-1 integer, needed later by the bitmap strategy
// (Roaring Bitmaps index integers, not UUIDs).
var Schema = []string{
	`CREATE TABLE IF NOT EXISTS users (
		id         CHAR(36)         NOT NULL,
		seq        INT UNSIGNED     NOT NULL,
		region     TINYINT UNSIGNED NOT NULL,
		age        TINYINT UNSIGNED NOT NULL,
		created_at DATETIME         NOT NULL,
		PRIMARY KEY (id) CLUSTERED,
		UNIQUE KEY uk_seq (seq)
	)`,
	`CREATE TABLE IF NOT EXISTS tags (
		id   INT         NOT NULL,
		name VARCHAR(64) NOT NULL,
		PRIMARY KEY (id) CLUSTERED
	)`,
	`CREATE TABLE IF NOT EXISTS user_tags (
		user_id CHAR(36) NOT NULL,
		tag_id  INT      NOT NULL,
		PRIMARY KEY (user_id, tag_id) CLUSTERED,
		KEY idx_tag_user (tag_id, user_id)
	)`,
}

var Drop = []string{
	"DROP TABLE IF EXISTS user_tags",
	"DROP TABLE IF EXISTS tags",
	"DROP TABLE IF EXISTS users",
}

var Analyze = []string{
	"ANALYZE TABLE users",
	"ANALYZE TABLE user_tags",
}
