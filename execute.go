package event

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

type ExecuteConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type ExecuteOption func(*ExecuteConfig)

func WithExecuteMaxAttempts(n int) ExecuteOption {
	return func(c *ExecuteConfig) { c.MaxAttempts = n }
}

func WithExecuteBackoff(baseDelay, maxDelay time.Duration) ExecuteOption {
	return func(c *ExecuteConfig) {
		c.BaseDelay = baseDelay
		c.MaxDelay = maxDelay
	}
}

// Execute loads a fresh aggregate, runs mutate, and saves the produced events
// with optimistic concurrency. newAggregate is called for every retry so a
// failed attempt never leaks partially applied state into the next attempt.
func Execute[A ESAggregate](
	ctx context.Context,
	store Store,
	newAggregate func() A,
	mutate func(context.Context, A, uint64, uint64) ([]Event, error),
	opts ...ExecuteOption,
) error {
	cfg := ExecuteConfig{
		MaxAttempts: 10,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 10 * time.Millisecond
	}
	if cfg.MaxDelay < cfg.BaseDelay {
		cfg.MaxDelay = cfg.BaseDelay
	}

	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleepBackoff(ctx, cfg, attempt); err != nil {
				return err
			}
		}

		aggregate := newAggregate()
		version, err := store.Load(ctx, aggregate)
		if err != nil {
			return fmt.Errorf("load aggregate: %w", err)
		}
		events, err := mutate(ctx, aggregate, version, uint64(attempt))
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		if err := store.Save(ctx, aggregate, version, events...); err != nil {
			if errors.Is(err, ErrConflict) {
				lastErr = err
				continue
			}
			return fmt.Errorf("save events: %w", err)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = ErrConflict
	}
	return fmt.Errorf("execute retries exhausted: %w", lastErr)
}

func sleepBackoff(ctx context.Context, cfg ExecuteConfig, attempt int) error {
	delay := min(cfg.BaseDelay*(1<<min(attempt, 30)), cfg.MaxDelay)
	if delay > 0 {
		delay = time.Duration(rand.Int64N(int64(delay)))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
