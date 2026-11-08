package store

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func legacyTime(t *testing.T, value string) time.Time {
	t.Helper()
	result, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func legacyExec(t *testing.T, s *Store, query string, args ...any) int64 {
	t.Helper()
	result, err := s.db.Exec(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestLegacyMistralHistoryInstantBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values [][2]string
		start  string
		end    string
		limit  int
		want   []string
	}{
		{
			name: "UTC whole-second start and fractional end",
			values: [][2]string{
				{"before", "2026-01-02T03:04:04.999999999Z"},
				{"start", "2026-01-02T03:04:05Z"},
				{"end", "2026-01-02T03:04:05.000000001Z"},
				{"after", "2026-01-02T03:04:05.000000002Z"},
			},
			start: "2026-01-02T03:04:05Z",
			end:   "2026-01-02T03:04:05.000000001Z",
			limit: 200,
			want:  []string{"end", "start"},
		},
		{
			name: "inclusive mixed offsets and nanoseconds",
			values: [][2]string{
				{"before", "2026-01-02T03:04:04.999999999Z"},
				{"whole UTC", "2026-01-02T03:04:05Z"},
				{"equal positive offset", "2026-01-02T06:04:05+03:00"},
				{"inside negative offset", "2026-01-01T22:04:05.000000001-05:00"},
				{"end UTC", "2026-01-02T03:04:05.000000002Z"},
				{"after", "2026-01-02T03:04:05.000000003Z"},
			},
			start: "2026-01-02T03:04:05Z",
			end:   "2026-01-02T03:04:05.000000002Z",
			limit: 200,
			want:  []string{"end UTC", "equal positive offset", "inside negative offset", "whole UTC"},
		},
		{
			name: "limit selects latest instant",
			values: [][2]string{
				{"newer", "2026-01-02T02:00:00-03:00"},
				{"older", "2026-01-02T05:00:00+01:00"},
			},
			start: "2026-01-02T00:00:00Z",
			end:   "2026-01-02T08:00:00Z",
			limit: 1,
			want:  []string{"newer"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.SetSetting("mistral_identity", "a"); err != nil {
				t.Fatal(err)
			}
			for _, value := range tc.values {
				legacyExec(t, s, `INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a',?,?)`, value[1], value[0])
			}
			// Rows were written in the formats accepted by the previous store,
			// independently of how a future SaveMistral formats new records.
			rows, err := s.MistralHistory(context.Background(), legacyTime(t, tc.start), legacyTime(t, tc.end), tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, row := range rows {
				got = append(got, row.Status)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("history = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLegacyMistralPruneInstantCutoff(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, row := range [][2]string{
		{"old UTC", "2026-01-02T00:00:00Z"},
		{"old offset", "2026-01-01T20:00:00-04:00"},
		{"at cutoff", "2026-01-01T19:00:00.000000001-05:00"},
		{"after cutoff", "2026-01-01T20:00:00.000000002-04:00"},
		{"latest", "2026-01-02T02:00:00Z"},
	} {
		legacyExec(t, s, `INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a',?,?)`, row[1], row[0])
	}
	for _, row := range [][2]string{
		{"old", "2026-01-02T00:00:00Z"},
		{"equal", "2026-01-01T19:00:00.000000001-05:00"},
		{"after", "2026-01-01T20:00:00.000000002-04:00"},
	} {
		legacyExec(t, s, `INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a',?,'2025-01-01T00:00:00Z',?,0,0,0,1,?)`, row[0], row[1], row[1])
	}
	if err := s.PruneMistral(context.Background(), legacyTime(t, "2026-01-02T00:00:00.000000001Z")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`SELECT status FROM mistral_snapshots ORDER BY status`, []string{"after cutoff", "at cutoff", "latest"}},
		{`SELECT quota_name FROM mistral_cycles ORDER BY quota_name`, []string{"after", "equal"}},
	} {
		rows, err := s.db.Query(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			got = append(got, name)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestLegacyCodexFirstSnapshotInstantOrder(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	legacyExec(t, s, `INSERT INTO codex_reset_cycles(account_id,quota_name,cycle_start,cycle_end,peak_utilization) VALUES(101,'weekly','2026-01-02T00:00:00Z','2026-01-02T08:00:00Z',90)`)
	for _, sample := range []struct {
		stamp string
		value float64
	}{
		{"2026-01-02T04:10:00+01:00", 10}, // 03:10 UTC
		{"2026-01-02T03:20:00Z", 90},
	} {
		id := legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(101,?)`, sample.stamp)
		legacyExec(t, s, `INSERT INTO codex_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'weekly',?)`, id, sample.value)
	}
	id := legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(202,'2026-01-02T03:00:00Z')`)
	legacyExec(t, s, `INSERT INTO codex_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'weekly',100)`, id)
	rows, err := s.QueryCodexCycleOverview(101, "weekly", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].CrossQuotas) != 1 {
		t.Fatalf("overview = %+v, want one account-scoped quota", rows)
	}
	if got := rows[0].CrossQuotas[0]; got.Percent != 90 || got.StartPercent != 10 || got.Delta != 80 {
		t.Fatalf("quota = %+v, want peak 90, start 10, delta 80", got)
	}
}

func TestLegacyCycleOverviewInstantBoundaries(t *testing.T) {
	for _, provider := range []string{"mistral", "codex", "opencode", "commandcode"} {
		t.Run(provider, func(t *testing.T) {
			s, err := New(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			const start = "2026-01-02T03:04:04Z"
			const end = "2026-01-02T03:04:05.000000002Z"
			switch provider {
			case "mistral":
				if err := s.SetSetting("mistral_identity", "a"); err != nil {
					t.Fatal(err)
				}
				legacyExec(t, s, `INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','weekly',?,?,99,0,0,1,?)`, start, end, end)
			case "codex":
				legacyExec(t, s, `INSERT INTO codex_reset_cycles(account_id,quota_name,cycle_start,cycle_end,peak_utilization) VALUES(101,'weekly',?,?,99)`, start, end)
			case "opencode", "commandcode":
				legacyExec(t, s, fmt.Sprintf(`INSERT INTO %s_reset_cycles(quota_name,cycle_start,cycle_end,peak_utilization) VALUES('weekly',?,?,99)`, provider), start, end)
			}
			for _, sample := range []struct {
				stamp string
				value float64
			}{
				{"2026-01-02T03:04:04Z", 10},
				{"2026-01-02T06:04:04.5+03:00", 20},
				{"2026-01-02T03:04:05Z", 80},
				{"2026-01-01T22:04:05.000000001-05:00", 90},
				{"2026-01-02T03:04:05.000000002Z", 99}, // exclusive end
			} {
				var id int64
				switch provider {
				case "mistral":
					id = legacyExec(t, s, `INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a',?,'ok')`, sample.stamp)
					legacyExec(t, s, `INSERT INTO mistral_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'weekly',?)`, id, sample.value)
				case "codex":
					id = legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(101,?)`, sample.stamp)
					legacyExec(t, s, `INSERT INTO codex_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'weekly',?)`, id, sample.value)
				case "opencode", "commandcode":
					id = legacyExec(t, s, fmt.Sprintf(`INSERT INTO %s_snapshots(captured_at) VALUES(?)`, provider), sample.stamp)
					legacyExec(t, s, fmt.Sprintf(`INSERT INTO %s_quota_values(snapshot_id,quota_name,utilization,limit_value) VALUES(?,'weekly',?,100)`, provider), id, sample.value)
				}
			}
			if provider == "codex" {
				id := legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(202,'2026-01-02T03:04:04.9Z')`)
				legacyExec(t, s, `INSERT INTO codex_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'weekly',100)`, id)
			}
			var rows []CycleOverviewRow
			switch provider {
			case "mistral":
				rows, err = s.MistralCycleOverview(context.Background(), "weekly")
			case "codex":
				rows, err = s.QueryCodexCycleOverview(101, "weekly", 10)
			case "opencode":
				rows, err = s.QueryOpenCodeCycleOverview("weekly", 10)
			case "commandcode":
				rows, err = s.QueryCommandCodeCycleOverview("weekly", 10)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || len(rows[0].CrossQuotas) != 1 {
				t.Fatalf("overview = %+v, want one cycle with one peak quota", rows)
			}
			if got := rows[0].CrossQuotas[0].Percent; got != 90 {
				t.Errorf("peak = %v, want 90 (end is exclusive)", got)
			}
			if !rows[0].PeakTime.Equal(legacyTime(t, "2026-01-02T03:04:05.000000001Z")) {
				t.Errorf("peak time = %s, want the instant 1ns before end", rows[0].PeakTime)
			}
			if provider == "codex" && rows[0].CrossQuotas[0].StartPercent != 10 {
				t.Errorf("start = %v, want 10 from the earliest account-scoped snapshot", rows[0].CrossQuotas[0].StartPercent)
			}
		})
	}
}
