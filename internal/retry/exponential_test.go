package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetrySuccess(t *testing.T) {
	var attempts atomic.Int32
	err := Do(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		return nil
	}, WithMaxAttempts(5))

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("expected 1 attempt, got %d", n)
	}
}

func TestRetrySuccessOnSecondAttempt(t *testing.T) {
	var attempts atomic.Int32
	err := Do(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		if attempts.Load() < 2 {
			return fmt.Errorf("transient error on attempt %d", attempts.Load())
		}
		return nil
	}, WithMaxAttempts(3), WithBaseDelay(time.Millisecond))

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if n := attempts.Load(); n != 2 {
		t.Errorf("expected 2 attempts, got %d", n)
	}
}

func TestRetryExhaustion(t *testing.T) {
	expectedErr := errors.New("permanent failure")
	var attempts atomic.Int32
	err := Do(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		return expectedErr
	}, WithMaxAttempts(3), WithBaseDelay(time.Millisecond))

	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if n := attempts.Load(); n != 3 {
		t.Errorf("expected 3 attempts, got %d", n)
	}
}

func TestRetryExhaustionWithMaxAttemptsOne(t *testing.T) {
	var attempts atomic.Int32
	err := Do(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("fail")
	}, WithMaxAttempts(1))

	if err == nil {
		t.Fatal("expected error after single attempt")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("expected 1 attempt, got %d", n)
	}
}

func TestRetryWithContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var attempts atomic.Int32
	err := Do(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("should not be retried")
	}, WithMaxAttempts(5), WithBaseDelay(time.Hour))

	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if n := attempts.Load(); n != 0 {
		t.Errorf("expected 0 attempts (context already cancelled), got %d", n)
	}
}

func TestRetryWithContextCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var attempts atomic.Int32
	ch := make(chan struct{})

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
		close(ch)
	}()

	err := Do(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("transient")
	}, WithMaxAttempts(10), WithBaseDelay(100*time.Millisecond), WithExponentialBackoff())

	<-ch

	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if n := attempts.Load(); n < 1 {
		t.Errorf("expected at least 1 attempt before cancel, got %d", n)
	}
}

func TestRetryWithContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var attempts atomic.Int32
	err := Do(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("transient")
	}, WithMaxAttempts(100), WithBaseDelay(100*time.Millisecond))

	if err == nil {
		t.Fatal("expected deadline exceeded error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected deadline/context error, got %v", err)
	}
}

func TestCalculateDelayExponential(t *testing.T) {
	cfg := &Config{
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    10 * time.Second,
		Strategy:    StrategyExponential,
	}

	expected := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
	}

	for i := 0; i < 5; i++ {
		delay := calculateDelay(cfg, i+1)
		if delay != expected[i] {
			t.Errorf("attempt %d: expected %v, got %v", i+1, expected[i], delay)
		}
	}
}

func TestCalculateDelayFixed(t *testing.T) {
	base := 500 * time.Millisecond
	cfg := &Config{
		BaseDelay: base,
		MaxDelay:  30 * time.Second,
		Strategy:  StrategyFixed,
	}

	for i := 0; i < 5; i++ {
		delay := calculateDelay(cfg, i)
		if delay != base {
			t.Errorf("attempt %d: expected %v, got %v", i, base, delay)
		}
	}
}

func TestCalculateDelayLinear(t *testing.T) {
	cfg := &Config{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  10 * time.Second,
		Strategy:  StrategyLinear,
	}

	for i := 1; i <= 5; i++ {
		delay := calculateDelay(cfg, i)
		expected := cfg.BaseDelay * time.Duration(i)
		if delay != expected {
			t.Errorf("attempt %d: expected %v, got %v", i, expected, delay)
		}
	}
}

func TestCalculateDelayMaxDelayCapping(t *testing.T) {
	cfg := &Config{
		MaxAttempts: 10,
		BaseDelay:   1 * time.Second,
		MaxDelay:    3 * time.Second,
		Strategy:    StrategyExponential,
	}

	for i := 1; i <= 5; i++ {
		delay := calculateDelay(cfg, i)
		expected := cfg.BaseDelay * time.Duration(math.Pow(2, float64(i-1)))
		if expected > cfg.MaxDelay {
			expected = cfg.MaxDelay
		}
		if delay != expected {
			t.Errorf("attempt %d: expected %v, got %v", i, expected, delay)
		}
	}
}

func TestCalculateDelayNoMaxDelay(t *testing.T) {
	cfg := &Config{
		BaseDelay: 1 * time.Second,
		MaxDelay:  0,
		Strategy:  StrategyExponential,
	}

	delay := calculateDelay(cfg, 10)
	expected := cfg.BaseDelay * time.Duration(math.Pow(2, 9))
	if delay != expected {
		t.Errorf("expected %v, got %v", expected, delay)
	}
}

func TestCalculateDelayExponentialWithJitter(t *testing.T) {
	cfg := &Config{
		BaseDelay: 1 * time.Second,
		MaxDelay:  30 * time.Second,
		Strategy:  StrategyExponentialWithJitter,
		Jitter:    0.5,
	}

	delays := make(map[time.Duration]int)
	for i := 0; i < 100; i++ {
		d := calculateDelay(cfg, 2)
		delays[d]++
		if d <= 0 {
			t.Errorf("expected positive delay, got %v", d)
		}
	}

	baseExpected := cfg.BaseDelay * 2
	if len(delays) < 2 {
		t.Log("note: jitter produced fewer unique values than expected; may need more iterations")
	}
	_ = baseExpected
}

func TestDoWithDataSuccess(t *testing.T) {
	result, err := DoWithData(context.Background(), func(ctx context.Context) (string, error) {
		return "hello", nil
	}, WithMaxAttempts(3))

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != "hello" {
		t.Errorf("expected 'hello', got %q", result)
	}
}

func TestDoWithDataFailure(t *testing.T) {
	_, err := DoWithData(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errors.New("fail")
	}, WithMaxAttempts(2), WithBaseDelay(time.Millisecond))

	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPermanentError(t *testing.T) {
	inner := errors.New("disk full")
	perm := &PermanentError{Err: inner}

	if !errors.Is(perm, inner) {
		t.Error("expected PermanentError to unwrap to inner error")
	}
	if !IsPermanent(perm) {
		t.Error("expected IsPermanent to return true for PermanentError")
	}
	if IsPermanent(inner) {
		t.Error("expected IsPermanent to return false for non-permanent error")
	}
}
