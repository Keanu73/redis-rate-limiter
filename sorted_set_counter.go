package redis_rate_limiter

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rueian/rueidis"
)

var (
	_ Strategy = &sortedSetCounter{}
)

const (
	sortedSetMax = "+inf"
	sortedSetMin = "-inf"
)

func NewSortedSetCounterStrategy(client rueidis.Client, now func() time.Time) Strategy {
	return &sortedSetCounter{
		client: client,
		now:    now,
	}
}

type sortedSetCounter struct {
	client rueidis.Client
	now    func() time.Time
}

// Run this implementation uses a sorted set that holds an UUID for every request with a score that is the
// time the request has happened. This allows us to delete events from *before* the current window, if the window
// is 5 minutes, we want to remove all events from before 5 minutes ago, this way we make sure we roll old
// requests off the counters creating a rolling window for the rate limiter. So, if your window is 100 requests
// in 5 minutes and you spread the load evenly across the minutes, once you hit 6 minutes the requests you made
// on the first minute have now expired but the other 4 minutes of requests are still valid.
// A rolling window counter is usually never 0 if traffic is consistent so it is very effective at preventing
// bursts of traffic as the counter won't ever expire.
func (s *sortedSetCounter) Run(ctx context.Context, r *Request) (*Result, error) {
	now := s.now()
	expiresAt := now.Add(r.Duration)
	minimum := now.Add(-r.Duration)

	// first count how many requests over the period we're tracking on this rolling window so check wether
	// we're already over the limit or not. this prevents new requests from being added if a client is already
	// rate limited, not allowing it to add an infinite amount of requests to the system overloading redis.
	// if the client continues to send requests it also means that the memory for this specific key will not
	// be reclaimed (as we're not writing data here) so make sure there is an eviction policy that will
	// clear up the memory if the redis starts to get close to its memory limit.

	// todo: make sure there is eviction policy redis
	result := s.client.Do(
		ctx, s.client.B().Zcount().Key(r.Key).Min(
			strconv.FormatInt(
				minimum.UnixMilli(),
				10,
			),
		).Max(sortedSetMax).Build(),
	)

	totalRequests, err := result.ToInt64()
	if result.Error() == nil && err == nil && uint64(totalRequests) >= r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: uint64(totalRequests),
			ExpiresAt:     expiresAt,
		}, nil
	}

	// every request needs an UUID
	item := uuid.New()

	// we then remove all requests that have already expired on this set
	removeByScore := s.client.B().Zremrangebyscore().Key(r.Key).Min("0").Max(
		strconv.FormatInt(
			minimum.UnixMilli(), 10,
		),
	).Build()

	// we add the current request
	add := s.client.B().Zadd().Key(r.Key).ScoreMember().ScoreMember(float64(now.UnixMilli()), item.String()).Build()

	// count how many non-expired requests we have on the sorted set
	count := s.client.B().Zcount().Key(r.Key).Min(sortedSetMin).Max(sortedSetMax).Build()

	// Execute both one after the other
	redisResult := s.client.DoMulti(ctx, removeByScore, add, count)
	for _, response := range redisResult {
		if response.Error() != nil {
			return nil, errors.Wrapf(response.Error(), "failed to execute sorted set pipeline for key: %v", r.Key)
		}
	}

	if err := redisResult[0].Error(); err != nil {
		return nil, errors.Wrapf(err, "failed to remove items from key %v", r.Key)
	}

	if err := redisResult[1].Error(); err != nil {
		return nil, errors.Wrapf(err, "failed to add item to key %v", r.Key)
	}

	totalRequests, err = redisResult[2].ToInt64()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to count items for key %v", r.Key)
	}

	requests := uint64(totalRequests)

	if requests > r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: requests,
			ExpiresAt:     expiresAt,
		}, nil
	}

	return &Result{
		State:         Allow,
		TotalRequests: requests,
		ExpiresAt:     expiresAt,
	}, nil
}
