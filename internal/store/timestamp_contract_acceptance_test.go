package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestTimestampContractMistralHistoryOrder(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetSetting("mistral_identity", "a"); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][2]string{
		{"before", "2026-01-02T03:04:04.999999999Z"},
		{"whole UTC", "2026-01-02T03:04:05Z"},
		{"equal positive offset", "2026-01-02T06:04:05+03:00"},
		{"inside negative offset", "2026-01-01T22:04:05.000000001-05:00"},
		{"end UTC", "2026-01-02T03:04:05.000000002Z"},
		{"after", "2026-01-02T03:04:05.000000003Z"},
	} {
		legacyExec(t, s, `INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES('a',?,?)`, row[1], row[0])
	}
	rows, err := s.MistralHistory(context.Background(), legacyTime(t, "2026-01-02T03:04:05Z"), legacyTime(t, "2026-01-02T03:04:05.000000002Z"), 200)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, row := range rows {
		got = append(got, row.Status)
	}
	want := []string{"whole UTC", "equal positive offset", "inside negative offset", "end UTC"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("history order = %v, want %v", got, want)
	}
}

func TestTimestampContractMistralPeakIdentity(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetSetting("mistral_identity", "a"); err != nil {
		t.Fatal(err)
	}
	legacyExec(t, s, `INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','weekly','2026-01-02T03:00:00Z','2026-01-02T04:00:00Z',60,0,0,1,'2026-01-02T03:30:00Z')`)
	for _, row := range []struct {
		identity, at string
		utilization  float64
	}{
		{"a", "2026-01-02T06:30:00+03:00", 60},
		{"b", "2026-01-01T22:45:00-05:00", 99},
	} {
		id := legacyExec(t, s, `INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES(?,?,'ok')`, row.identity, row.at)
		legacyExec(t, s, `INSERT INTO mistral_quota_values(snapshot_id,quota_name,utilization) VALUES(?,'weekly',?)`, id, row.utilization)
	}
	rows, err := s.MistralCycleOverview(context.Background(), "weekly")
	if err != nil || len(rows) != 1 {
		t.Fatalf("overview=%+v err=%v", rows, err)
	}
	if !rows[0].PeakTime.Equal(legacyTime(t, "2026-01-02T03:30:00Z")) || len(rows[0].CrossQuotas) != 1 || rows[0].CrossQuotas[0].Percent != 60 {
		t.Fatalf("identity-scoped peak=%+v, want a at 03:30 with 60", rows[0])
	}
}

func TestTimestampContractLegacyCycleOverviewsReopen(t *testing.T) {
	path := oldTimestampDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		start, end string
		peak       int
	}{
		{"2026-01-02T02:00:00-03:00", "2026-01-02T03:00:00-03:00", 20},
		{"2026-01-02T05:00:00+01:00", "2026-01-02T05:30:00+01:00", 10},
	} {
		if _, err := db.Exec(`INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','weekly',?,?,?,0,0,1,?)`, row.start, row.end, row.peak, row.end); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO codex_reset_cycles(account_id,quota_name,cycle_start,cycle_end,peak_utilization) VALUES(101,'weekly',?,?,?)`, row.start, row.end, row.peak); err != nil {
			t.Fatal(err)
		}
		for _, provider := range []string{"opencode", "commandcode"} {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s_reset_cycles(quota_name,cycle_start,cycle_end,peak_utilization) VALUES('weekly',?,?,?)`, provider), row.start, row.end, row.peak); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('b','weekly','2026-01-02T07:00:00Z','2026-01-02T08:00:00Z',99,0,0,1,'2026-01-02T08:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO codex_reset_cycles(account_id,quota_name,cycle_start,cycle_end,peak_utilization) VALUES(202,'weekly','2026-01-02T07:00:00Z','2026-01-02T08:00:00Z',99)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		s, err := New(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetSetting("mistral_identity", "a"); err != nil {
			t.Fatal(err)
		}
		m, em := s.MistralCycleOverview(context.Background(), "weekly")
		c, ec := s.QueryCodexCycleOverview(101, "weekly", 10)
		o, eo := s.QueryOpenCodeCycleOverview("weekly", 10)
		d, ed := s.QueryCommandCodeCycleOverview("weekly", 10)
		for _, result := range []struct {
			name string
			rows []CycleOverviewRow
			err  error
		}{
			{"mistral", m, em}, {"codex", c, ec}, {"opencode", o, eo}, {"commandcode", d, ed},
		} {
			if result.err != nil || len(result.rows) != 2 {
				t.Fatalf("pass %d %s rows=%+v err=%v", pass, result.name, result.rows, result.err)
			}
			for i, want := range [][2]string{{"2026-01-02T05:00:00Z", "2026-01-02T06:00:00Z"}, {"2026-01-02T04:00:00Z", "2026-01-02T04:30:00Z"}} {
				r := result.rows[i]
				if !r.CycleStart.Equal(legacyTime(t, want[0])) || r.CycleEnd == nil || !r.CycleEnd.Equal(legacyTime(t, want[1])) || r.PeakValue != float64(20-10*i) {
					t.Fatalf("pass %d %s[%d]=%+v, want %v", pass, result.name, i, r, want)
				}
			}
		}
		if err := s.SetSetting("mistral_identity", "b"); err != nil {
			t.Fatal(err)
		}
		m, em = s.MistralCycleOverview(context.Background(), "weekly")
		c, ec = s.QueryCodexCycleOverview(202, "weekly", 10)
		if em != nil || ec != nil || len(m) != 1 || len(c) != 1 || m[0].PeakValue != 99 || c[0].PeakValue != 99 {
			t.Fatalf("pass %d other scope mistral=%+v/%v codex=%+v/%v", pass, m, em, c, ec)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTimestampContractLegacyCycleCloseAndPrune(t *testing.T) {
	path := oldTimestampDB(t)
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetSetting("mistral_identity", "a"); err != nil {
		t.Fatal(err)
	}
	legacyExec(t, s, `INSERT INTO mistral_cycles(identity,quota_name,cycle_start,peak,delta,last_used,last_limit,last_at) VALUES('a','weekly','2026-01-01T00:00:00Z',0,0,0,1,'2026-01-01T01:00:00Z')`)
	legacyExec(t, s, `INSERT INTO mistral_cycles(identity,quota_name,cycle_start,cycle_end,peak,delta,last_used,last_limit,last_at) VALUES('a','weekly','2026-01-03T00:00:00Z','2026-01-04T00:00:00Z',0,0,0,1,'2026-01-03T01:00:00Z')`)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE mistral_cycles SET cycle_end='2026-01-01T21:00:00-03:00' WHERE identity='a' AND quota_name='weekly' AND cycle_end IS NULL`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.MistralCycles(context.Background(), "weekly", 10)
	if err != nil || len(rows) != 2 || rows[1].CycleEnd == nil || !rows[1].CycleEnd.Equal(legacyTime(t, "2026-01-02T00:00:00Z")) {
		t.Fatalf("cycles after legacy close=%+v err=%v", rows, err)
	}
	if err := s.PruneMistral(context.Background(), legacyTime(t, "2026-01-02T12:00:00Z")); err != nil {
		t.Fatal(err)
	}
	rows, err = s.MistralCycles(context.Background(), "weekly", 10)
	if err != nil || len(rows) != 1 || !rows[0].CycleStart.Equal(legacyTime(t, "2026-01-03T00:00:00Z")) || rows[0].CycleEnd == nil || !rows[0].CycleEnd.Equal(legacyTime(t, "2026-01-04T00:00:00Z")) {
		t.Fatalf("cycles after prune=%+v err=%v", rows, err)
	}
}

func TestTimestampContractMistralSparseRetention(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, identity := range []string{"a", "b"} {
		offset := 0.0
		if identity == "b" {
			offset = 100
		}
		for _, row := range []struct {
			status, at, quota string
			value             float64
			billing           bool
		}{
			{"latest", "2026-01-02T01:00:00-05:00", "", 0, false},
			{"api", "2026-01-02T02:00:00-03:00", "api_included", 10 + offset, false},
			{"vibe", "2026-01-02T07:30:00+03:00", "vibe_included", 20 + offset, false},
			{"billing", "2026-01-02T07:00:00+03:00", "", 30 + offset, true},
			{"obsolete", "2026-01-02T08:00:00+05:00", "both", 1 + offset, true},
		} {
			id := legacyExec(t, s, `INSERT INTO mistral_snapshots(identity,captured_at,status) VALUES(?,?,?)`, identity, row.at, row.status)
			if row.quota == "both" {
				for _, name := range []string{"api_included", "vibe_included"} {
					legacyExec(t, s, `INSERT INTO mistral_quota_values(snapshot_id,quota_name,utilization) VALUES(?,?,?)`, id, name, row.value)
				}
			} else if row.quota != "" {
				legacyExec(t, s, `INSERT INTO mistral_quota_values(snapshot_id,quota_name,utilization) VALUES(?,?,?)`, id, row.quota, row.value)
			}
			if row.billing {
				legacyExec(t, s, `INSERT INTO mistral_billing(snapshot_id,amount,currency,period_start,period_end,captured_at,status) VALUES(?,?,'EUR',?,?,?,'ok')`, id, row.value, row.at, row.at, row.at)
			}
		}
	}
	if err := s.PruneMistral(context.Background(), legacyTime(t, "2026-01-03T00:00:00Z")); err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"a", "b"} {
		offset := 0.0
		if identity == "b" {
			offset = 100
		}
		if err := s.SetSetting("mistral_identity", identity); err != nil {
			t.Fatal(err)
		}
		got, err := s.LatestMistral(context.Background())
		if err != nil || got == nil || got.Identity != identity || got.Status != "latest" || len(got.Quotas) != 2 || got.Billing == nil || got.Billing.Amount == nil {
			t.Fatalf("%s latest=%+v err=%v", identity, got, err)
		}
		if got.Quotas[0].Name != "api_included" || got.Quotas[0].Utilization != 10+offset || !got.Quotas[0].CapturedAt.Equal(legacyTime(t, "2026-01-02T05:00:00Z")) || got.Quotas[1].Name != "vibe_included" || got.Quotas[1].Utilization != 20+offset || !got.Quotas[1].CapturedAt.Equal(legacyTime(t, "2026-01-02T04:30:00Z")) || *got.Billing.Amount != 30+offset {
			t.Fatalf("%s sparse latest=%+v billing=%+v", identity, got.Quotas, got.Billing)
		}
		history, err := s.MistralHistory(context.Background(), legacyTime(t, "2026-01-02T00:00:00Z"), legacyTime(t, "2026-01-03T00:00:00Z"), 20)
		if err != nil {
			t.Fatal(err)
		}
		var statuses []string
		for _, row := range history {
			statuses = append(statuses, row.Status)
		}
		if want := []string{"billing", "vibe", "api", "latest"}; !reflect.DeepEqual(statuses, want) {
			t.Fatalf("%s retained=%v, want %v", identity, statuses, want)
		}
	}
}

func TestTimestampContractThreeProviderNanosecondRanges(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, at := range []string{
		"2026-01-01T22:04:05.000000008-05:00",
		"2026-01-02T03:04:05.000000007Z",
		"2026-01-02T06:04:05.000000006+03:00",
		"2026-01-01T22:04:05.000000005-05:00",
		"2026-01-02T03:04:05.000000004Z",
	} {
		legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(1,?)`, at)
		legacyExec(t, s, `INSERT INTO opencode_snapshots(captured_at) VALUES(?)`, at)
		legacyExec(t, s, `INSERT INTO commandcode_snapshots(captured_at) VALUES(?)`, at)
	}
	legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(2,'2026-01-02T03:04:05.000000006Z')`)
	start := legacyTime(t, "2026-01-02T03:04:05.000000005Z")
	end := start.Add(2 * time.Nanosecond)
	for _, limit := range []int{0, 10, 2} {
		var limits []int
		if limit > 0 {
			limits = []int{limit}
		}
		var ctimes, otimes, dtimes []time.Time
		c, cErr := s.QueryCodexRange(1, start, end, limits...)
		for _, row := range c {
			if row.AccountID != 1 {
				t.Fatalf("codex leaked account %d", row.AccountID)
			}
			ctimes = append(ctimes, row.CapturedAt)
		}
		o, oErr := s.QueryOpenCodeRange(start, end, limits...)
		for _, row := range o {
			otimes = append(otimes, row.CapturedAt)
		}
		d, dErr := s.QueryCommandCodeRange(start, end, limits...)
		for _, row := range d {
			dtimes = append(dtimes, row.CapturedAt)
		}
		for _, result := range []struct {
			name  string
			times []time.Time
			err   error
		}{{"codex", ctimes, cErr}, {"opencode", otimes, oErr}, {"commandcode", dtimes, dErr}} {
			want := []time.Time{start, start.Add(time.Nanosecond), end}
			if limit == 2 {
				want = want[1:]
			}
			if result.err != nil || len(result.times) != len(want) {
				t.Fatalf("%s limit=%d times=%v err=%v", result.name, limit, result.times, result.err)
			}
			for i := range want {
				if !result.times[i].Equal(want[i]) {
					t.Fatalf("%s limit=%d times=%v want=%v", result.name, limit, result.times, want)
				}
			}
		}
	}
}

func TestTimestampContractMixedOffsetDownsampling(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	for i := 200; i >= 0; i-- {
		at := start.Add(time.Duration(i)*time.Second + time.Duration(i)*time.Nanosecond)
		zone := time.FixedZone("plus", 3*60*60)
		if i%2 == 0 {
			zone = time.FixedZone("minus", -5*60*60)
		}
		raw := at.In(zone).Format(time.RFC3339Nano)
		legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(1,?)`, raw)
		legacyExec(t, s, `INSERT INTO opencode_snapshots(captured_at) VALUES(?)`, raw)
		legacyExec(t, s, `INSERT INTO commandcode_snapshots(captured_at) VALUES(?)`, raw)
	}
	legacyExec(t, s, `INSERT INTO codex_snapshots(account_id,captured_at) VALUES(2,?)`, start.Add(50*time.Second).Format(time.RFC3339Nano))
	end := start.Add(200*time.Second + 200*time.Nanosecond)
	c, ce := s.QueryCodexRangeSampled(1, start, end)
	o, oe := s.QueryOpenCodeChartRange(start, end)
	d, de := s.QueryCommandCodeChartRange(start, end)
	var ctimes, otimes, dtimes []time.Time
	for _, row := range c {
		if row.AccountID != 1 {
			t.Fatalf("codex leaked account %d", row.AccountID)
		}
		ctimes = append(ctimes, row.CapturedAt)
	}
	for _, row := range o {
		otimes = append(otimes, row.CapturedAt)
	}
	for _, row := range d {
		dtimes = append(dtimes, row.CapturedAt)
	}
	for _, result := range []struct {
		name  string
		times []time.Time
		err   error
	}{{"codex", ctimes, ce}, {"opencode", otimes, oe}, {"commandcode", dtimes, de}} {
		if result.err != nil || len(result.times) < 2 || len(result.times) > 200 {
			t.Fatalf("%s sampled count=%d err=%v", result.name, len(result.times), result.err)
		}
		if !result.times[0].Equal(start) || !result.times[len(result.times)-1].Equal(end) {
			t.Fatalf("%s endpoints=%v to %v", result.name, result.times[0], result.times[len(result.times)-1])
		}
		for i := 1; i < len(result.times); i++ {
			if !result.times[i].After(result.times[i-1]) {
				t.Fatalf("%s not ordered at %d: %v then %v", result.name, i, result.times[i-1], result.times[i])
			}
		}
	}
}
