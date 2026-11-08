package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Bias keeps every RFC3339 year and its offset positive while leaving seconds
// and nanoseconds separate. UnixNano cannot represent this range.
const instantSecondBias int64 = 1 << 62

func instantKey(t time.Time) string {
	return fmt.Sprintf("%019d%09d", t.Unix()+instantSecondBias, t.Nanosecond())
}

// sqliteInstantKey uses integral calendar days and whole seconds. Fractions
// are copied as digits rather than converted through SQLite's date functions.
// Invalid values become NULL, so they cannot be accidentally pruned.
func sqliteInstantKey(v string) string {
	day := fmt.Sprintf("substr(%s,1,10)", v)
	zone := fmt.Sprintf("(CASE WHEN substr(%s,-1)='Z' THEN 'Z' ELSE substr(%s,-6) END)", v, v)
	frac := fmt.Sprintf("substr(%s,20,length(%s)-19-length(%s))", v, v, zone)
	n := func(start, length int) string {
		return fmt.Sprintf("CAST(substr(%s,%d,%d) AS INTEGER)", v, start, length)
	}
	tz := fmt.Sprintf("(CASE WHEN %s='Z' THEN 0 ELSE (CASE substr(%s,1,1) WHEN '+' THEN 1 ELSE -1 END) * (CAST(substr(%s,2,2) AS INTEGER)*3600 + CAST(substr(%s,5,2) AS INTEGER)*60) END)", zone, zone, zone, zone)
	return fmt.Sprintf(`CASE WHEN
        length(%[1]s)>=20 AND substr(%[1]s,1,19) GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]'
        AND strftime('%%Y-%%m-%%d',%[2]s,'+0 days')=%[2]s
        AND %[3]s<=23 AND %[4]s<=59 AND %[5]s<=59
        AND (%[6]s='Z' OR (%[6]s GLOB '[+-][0-9][0-9]:[0-9][0-9]' AND CAST(substr(%[6]s,2,2) AS INTEGER)<=24 AND CAST(substr(%[6]s,5,2) AS INTEGER)<=60))
        AND (%[7]s='' OR (substr(%[7]s,1,1) IN ('.',',') AND length(%[7]s)>1 AND substr(%[7]s,2) NOT GLOB '*[^0-9]*'))
        THEN printf('%%019d%%09d', CAST((julianday(%[2]s)-2440587.5)*86400 AS INTEGER)+%[3]s*3600+%[4]s*60+%[5]s-%[8]s+%[9]d, CAST(substr(substr(%[7]s,2)||'000000000',1,9) AS INTEGER)) END`, v, day, n(12, 2), n(15, 2), n(18, 2), zone, frac, tz, instantSecondBias)
}

// migrateInstantKeys is additive and transactional. Built-in SQLite triggers
// also cover connections from older versions that know only the raw columns.
func (s *Store) migrateInstantKeys(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	type table struct {
		name    string
		columns []string
		index   string
	}
	tables := []table{
		{"mistral_snapshots", []string{"captured_at"}, "identity, captured_at_instant"},
		{"codex_snapshots", []string{"captured_at"}, "account_id, captured_at_instant"},
		{"opencode_snapshots", []string{"captured_at"}, "captured_at_instant"},
		{"commandcode_snapshots", []string{"captured_at"}, "captured_at_instant"},
		{"mistral_cycles", []string{"cycle_start", "cycle_end", "resets_at", "last_at"}, "identity, quota_name, cycle_start_instant"},
		{"codex_reset_cycles", []string{"cycle_start", "cycle_end"}, "account_id, quota_name, cycle_start_instant"},
		{"opencode_reset_cycles", []string{"cycle_start", "cycle_end"}, "quota_name, cycle_start_instant"},
		{"commandcode_reset_cycles", []string{"cycle_start", "cycle_end"}, "quota_name, cycle_start_instant"},
	}
	added := make(map[string]bool, len(tables))
	for _, t := range tables {
		for _, col := range t.columns {
			_, err = tx.ExecContext(ctx, "ALTER TABLE "+t.name+" ADD COLUMN "+col+"_instant TEXT")
			if err == nil {
				added[t.name] = true
			}
			if err != nil && !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("add %s.%s instant: %w", t.name, col, err)
			}
		}
	}
	for _, t := range tables {
		var assign, missing, backfill []string
		for _, col := range t.columns {
			assign = append(assign, col+"_instant="+sqliteInstantKey("NEW."+col))
			missing = append(missing, "("+col+" IS NOT NULL AND "+col+"_instant IS NULL)")
			backfill = append(backfill, col+"_instant="+sqliteInstantKey(col))
		}
		needsBackfill := added[t.name]
		if !needsBackfill {
			var triggerCount int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN (?,?)`, "instant_"+t.name+"_insert", "instant_"+t.name+"_update").Scan(&triggerCount); err != nil {
				return fmt.Errorf("check %s triggers: %w", t.name, err)
			}
			if triggerCount != 2 {
				if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+t.name+")").Scan(&needsBackfill); err != nil {
					return fmt.Errorf("check %s rows: %w", t.name, err)
				}
			}
		}
		for _, event := range []string{"INSERT", "UPDATE OF " + strings.Join(t.columns, ",")} {
			trigger := "instant_" + t.name + "_insert"
			if event != "INSERT" {
				trigger = "instant_" + t.name + "_update"
			}
			statement := "CREATE TRIGGER IF NOT EXISTS " + trigger + " AFTER " + event + " ON " + t.name + " BEGIN UPDATE " + t.name + " SET " + strings.Join(assign, ",") + " WHERE id=NEW.id; END"
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("create %s: %w", trigger, err)
			}
		}
		// A failed schema initialization can leave fresh columns without triggers.
		// Recover raw-only rows once; malformed values remain NULL on later opens.
		if needsBackfill {
			if _, err = tx.ExecContext(ctx, "UPDATE "+t.name+" SET "+strings.Join(backfill, ",")+" WHERE "+strings.Join(missing, " OR ")); err != nil {
				return fmt.Errorf("backfill %s: %w", t.name, err)
			}
		}
		if _, err = tx.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_"+t.name+"_instant ON "+t.name+"("+t.index+")"); err != nil {
			return fmt.Errorf("index %s: %w", t.name, err)
		}
	}
	return tx.Commit()
}

func instantWindowChartQueryWhere(table, columns, extraWhere string) string {
	query := windowChartQueryWhere(table, columns, extraWhere)
	query = strings.ReplaceAll(query, "captured_at BETWEEN", "captured_at_instant BETWEEN")
	return strings.ReplaceAll(query, "ORDER BY captured_at, id", "ORDER BY captured_at_instant, id")
}
