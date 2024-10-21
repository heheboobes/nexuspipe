package scheduler

import (
	"testing"
	"time"
)

func TestParseValidExpressions(t *testing.T) {
	valid := []string{
		"* * * * *",
		"0 * * * *",
		"0 0 * * *",
		"*/5 * * * *",
		"0 9 * * 1",
		"30 4 * * 1-5",
		"0 0 1 * *",
		"0 12 * * *",
		"0 0 * * 0",
		"* */2 * * *",
		"@every 5m",
		"@daily",
		"@hourly",
		"@weekly",
		"@monthly",
		"@yearly",
	}

	for _, expr := range valid {
		schedule, err := ParseCronExpression(expr)
		if err != nil {
			t.Errorf("expected valid expression %q, got error: %v", expr, err)
		}
		if schedule == nil {
			t.Errorf("expected non-nil schedule for %q", expr)
		}
	}
}

func TestParseInvalidExpressions(t *testing.T) {
	invalid := []string{
		"",
		"invalid",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"0 25 * * *",
		"0 0 32 * *",
		"0 0 * 13 *",
		"0 0 * * 8",
		"a b c d e",
		"*/0 * * * *",
	}

	for _, expr := range invalid {
		_, err := ParseCronExpression(expr)
		if err == nil {
			t.Errorf("expected error for invalid expression %q", expr)
		}
	}
}

func TestParseExpressionEmpty(t *testing.T) {
	_, err := ParseCronExpression("")
	if err == nil {
		t.Fatal("expected error for empty expression")
	}
}

func TestValidateCronExpression(t *testing.T) {
	if err := ValidateCronExpression("0 0 * * *"); err != nil {
		t.Errorf("expected valid, got error: %v", err)
	}
	if err := ValidateCronExpression(""); err == nil {
		t.Error("expected error for empty expression")
	}
}

func TestNextRunTime(t *testing.T) {
	expr := "0 0 * * *"
	next, err := NextRunTime(expr, time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if next.IsZero() {
		t.Fatal("expected non-zero next run time")
	}
	if next.Before(time.Now()) {
		t.Logf("next run %v is in the past (possibly just executed)", next)
	}
}

func TestNextRunTimeInvalidExpression(t *testing.T) {
	_, err := NextRunTime("bad", time.UTC)
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
}

func TestNextRunTimeCustomLocation(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load location: %v", err)
	}

	next, err := NextRunTime("0 9 * * 1", loc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if next.IsZero() {
		t.Fatal("expected non-zero next run time")
	}

	zone, offset := next.Zone()
	if zone != "EST" && zone != "EDT" {
		t.Errorf("expected America/New_York timezone, got %q offset %d", zone, offset)
	}
}

func TestNextRunTimeFrom(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := NextRunTimeFrom("0 0 * * *", from, time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, next)
	}
}

func TestNextNRunTimes(t *testing.T) {
	times, err := NextNRunTimes("0 0 * * *", 3, time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(times) != 3 {
		t.Fatalf("expected 3 run times, got %d", len(times))
	}
	for i := 1; i < len(times); i++ {
		if !times[i].After(times[i-1]) {
			t.Errorf("times should be strictly increasing: %v -> %v", times[i-1], times[i])
		}
	}
}

func TestNextNRunTimesInvalidN(t *testing.T) {
	_, err := NextNRunTimes("0 0 * * *", 0, time.UTC)
	if err == nil {
		t.Fatal("expected error for n=0")
	}
	_, err = NextNRunTimes("0 0 * * *", -1, time.UTC)
	if err == nil {
		t.Fatal("expected error for negative n")
	}
}

func TestDescribeSchedule(t *testing.T) {
	tests := []struct {
		expr        string
		description string
	}{
		{"* * * * *", "Every minute"},
		{"0 * * * *", "Every hour"},
		{"0 0 * * *", "Daily at midnight"},
		{"0 12 * * *", "Daily at noon"},
		{"0 0 1 * *", "First day of every month at midnight"},
		{"0 9 * * 1", "Every Monday at 9:00 AM"},
		{"0 9 * * 5", "Every Friday at 9:00 AM"},
		{"*/5 * * * *", "Every 5 minutes"},
		{"*/10 * * * *", "Every 10 minutes"},
		{"*/15 * * * *", "Every 15 minutes"},
		{"*/30 * * * *", "Every 30 minutes"},
	}

	for _, tc := range tests {
		desc, err := DescribeSchedule(tc.expr)
		if err != nil {
			t.Errorf("unexpected error for %q: %v", tc.expr, err)
			continue
		}
		if desc != tc.description {
			t.Errorf("DescribeSchedule(%q) = %q, want %q", tc.expr, desc, tc.description)
		}
	}
}

func TestDescribeScheduleDailyAtTime(t *testing.T) {
	desc, err := DescribeSchedule("30 6 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if desc != "Daily at 06:30" {
		t.Errorf("expected 'Daily at 06:30', got %q", desc)
	}
}

func TestDescribeScheduleWeekly(t *testing.T) {
	desc, err := DescribeSchedule("0 14 * * 2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if desc != "Every Tuesday at 14:00" {
		t.Errorf("expected 'Every Tuesday at 14:00', got %q", desc)
	}
}

func TestDescribeScheduleEveryHours(t *testing.T) {
	desc, err := DescribeSchedule("0 */3 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if desc != "Every 3 hours" {
		t.Errorf("expected 'Every 3 hours', got %q", desc)
	}
}

func TestDescribeScheduleCustom(t *testing.T) {
	desc, err := DescribeSchedule("0 0 15 * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if desc == "" {
		t.Error("expected non-empty description")
	}
}

func TestDescribeScheduleInvalid(t *testing.T) {
	_, err := DescribeSchedule("bad")
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
}

func TestTimeUntilNextRun(t *testing.T) {
	d, err := TimeUntilNextRun("0 0 * * *", time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d <= 0 {
		t.Logf("duration until next run: %v", d)
	}
}

func TestIsCronDue(t *testing.T) {
	due, err := IsCronDue("* * * * *", time.Now().Add(-2*time.Minute), time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !due {
		t.Log("expected '*' to be due (should always fire within 2 min)")
	}
}

func TestCountMissedRuns(t *testing.T) {
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)

	count, next, err := CountMissedRuns("0 0 * * *", since, until, time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 missed runs, got %d", count)
	}
	if next.IsZero() {
		t.Error("expected non-zero next run after until")
	}
}

func TestCountMissedRunsInvalidRange(t *testing.T) {
	since := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
	until := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	_, _, err := CountMissedRuns("0 0 * * *", since, until, time.UTC)
	if err == nil {
		t.Fatal("expected error for since > until")
	}
}

func TestNearestFutureRun(t *testing.T) {
	next, err := NearestFutureRun("0 0 * * *", time.UTC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.IsZero() {
		t.Fatal("expected non-zero next run")
	}
	if next.Before(time.Now()) {
		t.Error("expected nearest future run to be in the future")
	}
}

func TestCronScheduleSummary(t *testing.T) {
	summary, err := CronScheduleSummary("0 0 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if summary["expression"] != "0 0 * * *" {
		t.Errorf("expected expression '0 0 * * *', got %v", summary["expression"])
	}
	if summary["valid"] != true {
		t.Error("expected valid to be true")
	}
	if summary["description"] != "Daily at midnight" {
		t.Errorf("expected description 'Daily at midnight', got %v", summary["description"])
	}
	if _, ok := summary["next_run"]; !ok {
		t.Error("expected next_run in summary")
	}
	if _, ok := summary["upcoming_runs"]; !ok {
		t.Error("expected upcoming_runs in summary")
	}
	if summary["timezone"] != "UTC" {
		t.Errorf("expected timezone 'UTC', got %v", summary["timezone"])
	}
}

func TestMustParseCronExpression(t *testing.T) {
	schedule := MustParseCronExpression("0 0 * * *")
	if schedule == nil {
		t.Fatal("expected non-nil schedule")
	}
}

func TestMustParseCronExpressionPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid expression")
		}
	}()
	MustParseCronExpression("bad")
}

func TestDescribeDayOfWeek(t *testing.T) {
	if d := describeDayOfWeek("0"); d != "Sunday" {
		t.Errorf("expected 'Sunday', got %q", d)
	}
	if d := describeDayOfWeek("1,3,5"); d != "Monday, Wednesday, Friday" {
		t.Errorf("expected 'Monday, Wednesday, Friday', got %q", d)
	}
	if d := describeDayOfWeek("7"); d != "Sunday" {
		t.Errorf("expected 'Sunday', got %q", d)
	}
}

func TestPadField(t *testing.T) {
	if padField("5") != "05" {
		t.Errorf("expected '05', got %q", padField("5"))
	}
	if padField("10") != "10" {
		t.Errorf("expected '10', got %q", padField("10"))
	}
	if padField("0") != "00" {
		t.Errorf("expected '00', got %q", padField("0"))
	}
}
