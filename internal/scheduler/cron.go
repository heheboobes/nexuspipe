package scheduler

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var defaultParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func ParseCronExpression(expr string) (cron.Schedule, error) {
	if expr == "" {
		return nil, fmt.Errorf("cron expression cannot be empty")
	}
	schedule, err := defaultParser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}
	return schedule, nil
}

func MustParseCronExpression(expr string) cron.Schedule {
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		panic(err)
	}
	return schedule
}

func NextRunTime(expr string, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return time.Time{}, err
	}
	return schedule.Next(time.Now().In(loc)), nil
}

func NextRunTimeFrom(expr string, from time.Time, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return time.Time{}, err
	}
	next := schedule.Next(from.In(loc))
	return next, nil
}

func NextNRunTimes(expr string, n int, loc *time.Location) ([]time.Time, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be positive, got %d", n)
	}
	if loc == nil {
		loc = time.UTC
	}
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return nil, err
	}
	times := make([]time.Time, 0, n)
	next := time.Now().In(loc)
	for i := 0; i < n; i++ {
		next = schedule.Next(next)
		times = append(times, next)
	}
	return times, nil
}

func ValidateCronExpression(expr string) error {
	_, err := ParseCronExpression(expr)
	return err
}

var weekdayNames = map[time.Weekday]string{
	time.Sunday:    "Sunday",
	time.Monday:    "Monday",
	time.Tuesday:   "Tuesday",
	time.Wednesday: "Wednesday",
	time.Thursday:  "Thursday",
	time.Friday:    "Friday",
	time.Saturday:  "Saturday",
}

var monthNames = map[time.Month]string{
	time.January:   "January",
	time.February:  "February",
	time.March:     "March",
	time.April:     "April",
	time.May:       "May",
	time.June:      "June",
	time.July:      "July",
	time.August:    "August",
	time.September: "September",
	time.October:   "October",
	time.November:  "November",
	time.December:  "December",
}

func DescribeSchedule(expr string) (string, error) {
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return "", err
	}
	return describeSchedule(schedule, expr), nil
}

func describeSchedule(_ cron.Schedule, expr string) string {
	fields := strings.Fields(expr)
	if len(fields) != 5 && len(fields) != 6 {
		return fmt.Sprintf("Custom schedule: %s", expr)
	}

	if len(fields) == 6 {
		fields = fields[1:]
	}

	minute := fields[0]
	hour := fields[1]
	dom := fields[2]
	month := fields[3]
	dow := fields[4]

	if minute == "*" && hour == "*" && dom == "*" && month == "*" && dow == "*" {
		return "Every minute"
	}
	if minute == "0" && hour == "*" && dom == "*" && month == "*" && dow == "*" {
		return "Every hour"
	}
	if minute == "0" && hour == "0" && dom == "*" && month == "*" && dow == "*" {
		return "Daily at midnight"
	}
	if minute == "0" && hour == "12" && dom == "*" && month == "*" && dow == "*" {
		return "Daily at noon"
	}
	if minute == "0" && hour == "0" && dom == "1" && month == "*" && dow == "*" {
		return "First day of every month at midnight"
	}
	if minute == "0" && hour == "9" && dom == "*" && month == "*" && dow == "1" {
		return "Every Monday at 9:00 AM"
	}
	if minute == "0" && hour == "9" && dom == "*" && month == "*" && dow == "5" {
		return "Every Friday at 9:00 AM"
	}
	if dom == "*" && month == "*" && dow != "*" && hour != "*" && minute != "*" {
		return fmt.Sprintf("Every %s at %s:%s", describeDayOfWeek(dow), padField(hour), padField(minute))
	}
	if dom == "*" && month == "*" && dow == "*" && hour != "*" && minute != "*" {
		return fmt.Sprintf("Daily at %s:%s", padField(hour), padField(minute))
	}
	if minute == "*/5" && hour == "*" {
		return "Every 5 minutes"
	}
	if minute == "*/10" && hour == "*" {
		return "Every 10 minutes"
	}
	if minute == "*/15" && hour == "*" {
		return "Every 15 minutes"
	}
	if minute == "*/30" && hour == "*" {
		return "Every 30 minutes"
	}
	if minute == "0" && strings.HasPrefix(hour, "*/") {
		return fmt.Sprintf("Every %s hours", strings.TrimPrefix(hour, "*/"))
	}

	return fmt.Sprintf("Cron: %s", expr)
}

func describeDayOfWeek(dow string) string {
	days := strings.Split(dow, ",")
	names := make([]string, 0, len(days))
	for _, d := range days {
		d = strings.TrimSpace(d)
		switch d {
		case "0", "7":
			names = append(names, "Sunday")
		case "1":
			names = append(names, "Monday")
		case "2":
			names = append(names, "Tuesday")
		case "3":
			names = append(names, "Wednesday")
		case "4":
			names = append(names, "Thursday")
		case "5":
			names = append(names, "Friday")
		case "6":
			names = append(names, "Saturday")
		default:
			names = append(names, d)
		}
	}
	return strings.Join(names, ", ")
}

func padField(f string) string {
	if len(f) == 1 {
		return "0" + f
	}
	return f
}

func TimeUntilNextRun(expr string, loc *time.Location) (time.Duration, error) {
	next, err := NextRunTime(expr, loc)
	if err != nil {
		return 0, err
	}
	return time.Until(next), nil
}

func IsCronDue(expr string, since time.Time, loc *time.Location) (bool, error) {
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return false, err
	}
	if loc == nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	next := schedule.Next(since.In(loc))
	return !next.After(now), nil
}

func CountMissedRuns(expr string, since time.Time, until time.Time, loc *time.Location) (int, time.Time, error) {
	if since.After(until) {
		return 0, time.Time{}, fmt.Errorf("since must be before until")
	}
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return 0, time.Time{}, err
	}
	if loc == nil {
		loc = time.UTC
	}
	count := 0
	next := since.In(loc)
	for {
		next = schedule.Next(next)
		if next.After(until) {
			break
		}
		count++
	}
	return count, next, nil
}

func NearestFutureRun(expr string, loc *time.Location) (time.Time, error) {
	next, err := NextRunTime(expr, loc)
	if err != nil {
		return time.Time{}, err
	}
	now := time.Now()
	if loc != nil {
		now = now.In(loc)
	}
	if next.Before(now) {
		return NextRunTimeFrom(expr, next.Add(time.Second), loc)
	}
	return next, nil
}

func CronScheduleSummary(expr string) (map[string]any, error) {
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	nextFive, _ := NextNRunTimes(expr, 5, time.UTC)
	nextStr := make([]string, len(nextFive))
	for i, t := range nextFive {
		nextStr[i] = t.Format(time.RFC3339)
	}
	description, _ := DescribeSchedule(expr)

	nextDate := schedule.Next(now)
	nextDow := weekdayNames[nextDate.Weekday()]

	return map[string]any{
		"expression":    expr,
		"valid":         true,
		"description":   description,
		"next_run":      nextDate.Format(time.RFC3339),
		"next_dow":      nextDow,
		"timezone":      "UTC",
		"upcoming_runs": nextStr,
		"seconds":       0,
	}, nil
}

var _ = math.Max(float64(1), float64(2))
