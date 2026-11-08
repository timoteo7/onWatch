package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These are the four provider tables as written before instant keys existed.
func oldTimestampDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		`CREATE TABLE mistral_snapshots(id INTEGER PRIMARY KEY, identity TEXT NOT NULL, captured_at TEXT NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE mistral_cycles(id INTEGER PRIMARY KEY, identity TEXT NOT NULL, quota_name TEXT NOT NULL, cycle_start TEXT NOT NULL, cycle_end TEXT, resets_at TEXT, peak REAL NOT NULL, delta REAL NOT NULL, last_used REAL NOT NULL, last_limit REAL NOT NULL, last_at TEXT NOT NULL)`,
		`CREATE TABLE codex_snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT, captured_at TEXT NOT NULL, account_id INTEGER NOT NULL DEFAULT 1, plan_type TEXT, credits_balance REAL, reset_credits_count INTEGER, raw_json TEXT, quota_count INTEGER DEFAULT 0)`,
		`CREATE TABLE codex_reset_cycles(id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL DEFAULT 1, quota_name TEXT NOT NULL, cycle_start TEXT NOT NULL, cycle_end TEXT, resets_at TEXT, peak_utilization REAL NOT NULL DEFAULT 0, total_delta REAL NOT NULL DEFAULT 0)`,
		`CREATE TABLE opencode_snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT, captured_at TEXT NOT NULL, raw_json TEXT NOT NULL DEFAULT '', account_type TEXT NOT NULL DEFAULT '', plan_name TEXT NOT NULL DEFAULT '', quota_count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE opencode_reset_cycles(id INTEGER PRIMARY KEY AUTOINCREMENT, quota_name TEXT NOT NULL, cycle_start TEXT NOT NULL, cycle_end TEXT, resets_at TEXT, peak_utilization REAL NOT NULL DEFAULT 0, total_delta REAL NOT NULL DEFAULT 0)`,
		`CREATE TABLE commandcode_snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT, captured_at TEXT NOT NULL, raw_json TEXT NOT NULL DEFAULT '', account_name TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '', org_id TEXT NOT NULL DEFAULT '', plan TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '', monthly_credits REAL NOT NULL DEFAULT 0, purchased_credits REAL NOT NULL DEFAULT 0, free_credits REAL NOT NULL DEFAULT 0, remaining_credits REAL NOT NULL DEFAULT 0, period_start TEXT, period_end TEXT, period_cost REAL NOT NULL DEFAULT 0, period_requests INTEGER NOT NULL DEFAULT 0, period_tokens INTEGER NOT NULL DEFAULT 0, quota_count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE commandcode_reset_cycles(id INTEGER PRIMARY KEY AUTOINCREMENT, quota_name TEXT NOT NULL, cycle_start TEXT NOT NULL, cycle_end TEXT, resets_at TEXT, peak_utilization REAL NOT NULL DEFAULT 0, total_delta REAL NOT NULL DEFAULT 0)`,
		`INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a','2026-01-02T02:00:00-03:00','newer'),('a','2026-01-02T05:00:00+01:00','older'),('b','2026-01-02T06:00:00Z','other'),('a','not-a-timestamp','invalid')`,
		`INSERT INTO codex_snapshots(account_id,captured_at) VALUES(1,'2026-01-02T02:00:00-03:00'),(1,'2026-01-02T05:00:00+01:00'),(2,'2026-01-02T06:00:00Z')`,
		`INSERT INTO opencode_snapshots(captured_at,plan_name) VALUES('2026-01-02T02:00:00-03:00','newer'),('2026-01-02T05:00:00+01:00','older')`,
		`INSERT INTO commandcode_snapshots(captured_at,plan) VALUES('2026-01-02T02:00:00-03:00','newer'),('2026-01-02T05:00:00+01:00','older')`,
		`INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','invalid','2025-01-01T00:00:00Z','bad',0,0,0,1,'bad')`,
		`INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','invalid-start','bad','2026-01-01T00:00:00Z',0,0,0,1,'bad')`,
		`INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,resets_at,peak,delta,last_used,last_limit,last_at) VALUES('a','invalid-reset','2025-01-01T00:00:00Z','2026-01-01T00:00:00Z','bad',0,0,0,1,'2026-01-01T00:00:00Z')`,
		`INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','invalid-last','2025-01-01T00:00:00Z','2026-01-01T00:00:00Z',0,0,0,1,'bad')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("legacy setup: %v", err)
		}
	}
	return path
}

func TestTimestampMigrationReopenAndLegacyInserts(t *testing.T) {
	path := oldTimestampDB(t)
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetSetting("mistral_identity", "a"); err != nil {
		t.Fatal(err)
	}
	check := func(s *Store) {
		t.Helper()
		a, err := s.LatestMistral(context.Background())
		if err != nil || a == nil || a.Status != "newer" {
			t.Fatalf("mistral latest=%+v err=%v", a, err)
		}
		c, err := s.QueryLatestCodex(1)
		if err != nil || c == nil || !c.CapturedAt.Equal(time.Date(2026, 1, 2, 5, 0, 0, 0, time.UTC)) {
			t.Fatalf("codex latest=%+v err=%v", c, err)
		}
		o, err := s.QueryLatestOpenCode()
		if err != nil || o == nil || o.PlanName != "newer" {
			t.Fatalf("opencode latest=%+v err=%v", o, err)
		}
		d, err := s.QueryLatestCommandCode()
		if err != nil || d == nil || d.Plan != "newer" {
			t.Fatalf("commandcode latest=%+v err=%v", d, err)
		}
		var raw string
		if err := s.db.QueryRow(`SELECT captured_at FROM mistral_snapshots WHERE status='newer'`).Scan(&raw); err != nil || raw != "2026-01-02T02:00:00-03:00" {
			t.Fatalf("raw timestamp %q err=%v", raw, err)
		}
		if err := s.db.QueryRow(`SELECT cycle_end FROM mistral_cycles WHERE quota_name='invalid'`).Scan(&raw); err != nil || raw != "bad" {
			t.Fatalf("raw cycle timestamp %q err=%v", raw, err)
		}
	}
	check(s)
	if err := s.PruneMistral(context.Background(), time.Date(2026, 1, 2, 4, 30, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mistral_snapshots WHERE status='invalid'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("invalid snapshot lost: %d %v", n, err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mistral_cycles WHERE quota_name='invalid'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("invalid cycle lost: %d %v", n, err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM mistral_cycles WHERE quota_name='invalid-start'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("cycle with invalid start lost: %d %v", n, err)
	}
	for _, name := range []string{"invalid-reset", "invalid-last"} {
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM mistral_cycles WHERE quota_name=?`, name).Scan(&n); err != nil || n != 1 {
			t.Fatalf("cycle with %s lost: %d %v", name, n, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// An old version only supplies the original columns; its SQLite connection
	// has no custom functions or collations installed.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('other','2026-01-02T09:00:00+03:00','old-writer')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE mistral_snapshots SET captured_at='2026-01-02T05:30:00Z' WHERE status='old-writer'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		s, err = New(path)
		if err != nil {
			t.Fatal(err)
		}
		check(s)
		var updated sql.NullString
		if err = s.db.QueryRow(`SELECT captured_at_instant FROM mistral_snapshots WHERE status='old-writer'`).Scan(&updated); err != nil || !updated.Valid || updated.String != instantKey(time.Date(2026, 1, 2, 5, 30, 0, 0, time.UTC)) {
			t.Fatalf("old writer update key=%+v err=%v", updated, err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTimestampExactKeysAndMalformedValues(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, raw := range []string{
		"0000-01-01T00:00:00+23:59", "0001-01-01T00:00:00Z",
		"1969-12-31T23:59:59.999999999Z", "1970-01-01T00:00:00Z",
		"2026-01-02T03:04:05,123456789123-05:30", "2026-01-02T03:04:05+24:60",
		"9999-12-31T23:59:59.999999999-23:59",
	} {
		t.Run(raw, func(t *testing.T) {
			at, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				t.Fatal(err)
			}
			res, err := s.db.Exec(`INSERT INTO opencode_snapshots(captured_at) VALUES(?)`, raw)
			if err != nil {
				t.Fatal(err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				t.Fatal(err)
			}
			var key sql.NullString
			if err = s.db.QueryRow(`SELECT captured_at_instant FROM opencode_snapshots WHERE id=?`, id).Scan(&key); err != nil {
				t.Fatal(err)
			}
			if !key.Valid || key.String != instantKey(at) {
				t.Fatalf("instant key=%q valid=%v want=%q", key.String, key.Valid, instantKey(at))
			}
		})
	}
	for _, raw := range []string{"bad", "2026-02-30T00:00:00Z", "2026-01-02T03:04:05.abcZ", "2026-01-02T03:04:05+25:00"} {
		result, err := s.db.Exec(`INSERT INTO opencode_snapshots(captured_at) VALUES(?)`, raw)
		if err != nil {
			t.Fatal(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		var key sql.NullString
		if err = s.db.QueryRow(`SELECT captured_at_instant FROM opencode_snapshots WHERE id=?`, id).Scan(&key); err != nil || key.Valid {
			t.Fatalf("malformed %q key=%+v err=%v", raw, key, err)
		}
	}
}

func TestTimestampMigrationRollback(t *testing.T) {
	path := oldTimestampDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`DROP TABLE commandcode_snapshots`); err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db}
	if err = s.migrateInstantKeys(context.Background()); err == nil {
		t.Fatal("missing table must fail migration")
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM pragma_table_info('mistral_snapshots') WHERE name='captured_at_instant'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("migration did not roll back: %d %v", count, err)
	}
}

func TestTimestampMigrationCancellation(t *testing.T) {
	path := oldTimestampDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.migrateInstantKeys(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled migration error = %v, want context.Canceled", err)
	}
	if err := s.migrateInstantKeys(context.Background()); err != nil {
		t.Fatalf("migration with valid context failed: %v", err)
	}
}

// Limited range and chart reads must select and return instant order, even when
// insertion order and local timestamp text suggest different answers.
func TestTimestampMixedOffsetRangeAndChartOrder(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stamps := []string{"2026-01-02T04:00:00Z", "2026-01-02T02:00:00-03:00", "2026-01-02T05:30:00+02:30"}
	for _, raw := range stamps {
		for _, stmt := range []string{`INSERT INTO codex_snapshots(account_id,captured_at) VALUES(1,?)`, `INSERT INTO opencode_snapshots(captured_at) VALUES(?)`, `INSERT INTO commandcode_snapshots(captured_at) VALUES(?)`} {
			if _, err := s.db.Exec(stmt, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := s.db.Exec(`INSERT INTO codex_snapshots(account_id,captured_at) VALUES(2,'2026-01-02T06:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 2, 5, 0, 0, 0, time.UTC)
	want := []int{3, 4, 5}
	check := func(name string, times []time.Time, w []int) {
		t.Helper()
		if len(times) != len(w) {
			t.Fatalf("%s count=%d want=%d", name, len(times), len(w))
		}
		for i, at := range times {
			if at.UTC().Hour() != w[i] {
				t.Fatalf("%s[%d]=%s want hour %d", name, i, at, w[i])
			}
		}
	}
	a, err := s.QueryCodexRange(1, start, end, 2)
	if err != nil {
		t.Fatal(err)
	}
	var got []time.Time
	for _, v := range a {
		got = append(got, v.CapturedAt)
	}
	check("codex limited", got, want[1:])
	a, err = s.QueryCodexRangeSampled(1, start, end)
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, v := range a {
		got = append(got, v.CapturedAt)
	}
	check("codex chart", got, want)
	o, err := s.QueryOpenCodeRange(start, end, 2)
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, v := range o {
		got = append(got, v.CapturedAt)
	}
	check("opencode limited", got, want[1:])
	o, err = s.QueryOpenCodeChartRange(start, end)
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, v := range o {
		got = append(got, v.CapturedAt)
	}
	check("opencode chart", got, want)
	d, err := s.QueryCommandCodeRange(start, end, 2)
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, v := range d {
		got = append(got, v.CapturedAt)
	}
	check("commandcode limited", got, want[1:])
	d, err = s.QueryCommandCodeChartRange(start, end)
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, v := range d {
		got = append(got, v.CapturedAt)
	}
	check("commandcode chart", got, want)
}

func TestTimestampRetentionPreservesLatestByInstant(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, x := range []struct {
		stamp, status string
		quota         bool
	}{
		{"2026-01-02T05:00:00Z", "newer", true}, {"2026-01-02T04:00:00Z", "older", true}, {"2026-01-02T06:00:00Z", "latest", false},
	} {
		res, err := s.db.Exec(`INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a',?,?)`, x.stamp, x.status)
		if err != nil {
			t.Fatal(err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if x.quota {
			if _, err = s.db.Exec(`INSERT INTO mistral_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'api_included',1)`, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.PruneMistral(context.Background(), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var names string
	if err := s.db.QueryRow(`SELECT group_concat(status,',') FROM (SELECT status FROM mistral_snapshots ORDER BY status)`).Scan(&names); err != nil || names != "latest,newer" {
		t.Fatalf("retained %q err=%v", names, err)
	}
}

func TestTimestampNoHistoryHotSortAfterMigration(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	queries := []struct {
		query string
		args  []any
	}{
		{`SELECT id FROM codex_snapshots WHERE account_id=? AND captured_at_instant BETWEEN ? AND ? ORDER BY captured_at_instant DESC,id DESC LIMIT 1`, []any{1, instantKey(time.Time{}), instantKey(time.Now())}},
		{`SELECT id FROM opencode_snapshots WHERE captured_at_instant BETWEEN ? AND ? ORDER BY captured_at_instant DESC,id DESC LIMIT 1`, []any{instantKey(time.Time{}), instantKey(time.Now())}},
		{`SELECT id FROM commandcode_snapshots WHERE captured_at_instant BETWEEN ? AND ? ORDER BY captured_at_instant DESC,id DESC LIMIT 1`, []any{instantKey(time.Time{}), instantKey(time.Now())}},
	}
	for _, q := range queries {
		rows, err := s.db.Query("EXPLAIN QUERY PLAN "+q.query, q.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		for rows.Next() {
			var id, parent, aux int
			var detail string
			if err := rows.Scan(&id, &parent, &aux, &detail); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan += detail + " "
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		if strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, "_instant") {
			t.Fatalf("unindexed history plan: %s", plan)
		}
	}
}

func TestTimestampRetryAfterFailedSchemaInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE mistral_cycles(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := New(path)
	if err == nil {
		s.Close()
		t.Fatal("expected initialization to fail for the incomplete cycles table")
	}
	t.Logf("initial initialization failed as expected: %v", err)

	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE mistral_cycles`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a','2026-01-02T02:00:00.123456789-03:00','raw-only')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	want := time.Date(2026, 1, 2, 5, 0, 0, 123456789, time.UTC)
	for i := 0; i < 2; i++ {
		s, err := New(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if err := s.SetSetting("mistral_identity", "a"); err != nil {
			t.Fatal(err)
		}
		latest, err := s.LatestMistral(context.Background())
		if err != nil || latest == nil || latest.Status != "raw-only" || !latest.CapturedAt.Equal(want) {
			t.Errorf("open %d latest=%+v err=%v, want raw-only at %s", i, latest, err, want)
		}
		history, err := s.MistralHistory(context.Background(), want.Add(-time.Nanosecond), want.Add(time.Nanosecond), 10)
		if err != nil || len(history) != 1 || history[0].Status != "raw-only" || !history[0].CapturedAt.Equal(want) {
			t.Errorf("open %d history=%+v err=%v, want raw-only at %s", i, history, err, want)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
