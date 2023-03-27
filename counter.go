package redis_rate_limiter

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rueian/rueidis"
)

var (
	_ Strategy = &CounterStrategy{}
)

const (
	keyThatDoesNotExist = -2
	keyWithoutExpire    = -1
)

func NewCounterStrategy(client rueidis.Client, now func() time.Time) *CounterStrategy {
	if client == nil {
		panic("client is nil")
	}
	return &CounterStrategy{
		client: client,
		now:    now,
	}
}

type CounterStrategy struct {
	client rueidis.Client
	now    func() time.Time
}

// Run this implementation uses a simple counter with an expiration set to the rate limit duration.
// This implementation is funtional but not very effective if you have to deal with bursty traffic as
// it will still allow a client to burn through it's full limit quickly once the key expires.
func (c *CounterStrategy) Run(ctx context.Context, r *Request) (*Result, error) {

	// a pipeline in redis is a way to send multiple commands that will all be run together.
	// this is not a transaction and there are many ways in which these commands could fail
	// (only the first, only the second) so we have to make sure all errors are handled, this
	// is a network performance optimization.

	// here we try to get the current value and also try to get its' TTL
	// note: rueidis auto-pipelines requests so we don't need to do it ourselves
	getQuery := c.client.B().Get().Key(r.Key).Build()
	ttlQuery := c.client.B().Ttl().Key(r.Key).Build()

	// [0] = getQuery, [1] = ttlQuery
	multiResult := c.client.DoMulti(ctx, getQuery, ttlQuery)
	for _, response := range multiResult {
		if response.Error() != nil {
			if !errors.Is(response.Error(), rueidis.Nil) {
				err := response.Error()
				return nil, errors.Wrapf(err, "failed to execute pipeline with get and ttl to key %v", r.Key)
			}
		}
	}

	var ttlDuration time.Duration

	// we want to make sure there is always an expiration set on the key, so on every
	// increment we check again to make sure it has a TTl and if it doesn't we add one.
	// a duration of -1 means that the key has no expiration so we need to make sure there
	// is one set, this should, most of the time, happen when we increment for the
	// first time but there could be cases where we fail at the previous commands so we should
	// check for the TTL on every request.
	// a duration of -2 means that the key does not exist, given we're already here we should set an expiration
	// to it anyway as it means this is a new key that will be incremented below.
	d, err := multiResult[1].ToInt64()
	dInt := int(d)
	if err = multiResult[1].Error(); err != nil || dInt == keyWithoutExpire || dInt == keyThatDoesNotExist {
		ttlDuration = r.Duration

		setExpireQuery := c.client.B().Expire().Key(r.Key).Seconds(int64(r.Duration.Seconds())).Build()
		if result := c.client.Do(ctx, setExpireQuery); result.Error() != nil {
			return nil, errors.Wrapf(result.Error(), "failed to set an expiration to key %v", r.Key)
		}
	} else {
		ttlDuration = time.Duration(d)
	}

	expiresAt := c.now().Add(ttlDuration)

	total, err := multiResult[0].ToInt64()

	if err != nil && errors.Is(err, rueidis.Nil) {

	} else if uint64(total) >= r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: uint64(total),
			ExpiresAt:     expiresAt,
		}, nil
	}

	incrResult := c.client.Do(ctx, c.client.B().Incr().Key(r.Key).Build())

	totalRequests, err := incrResult.ToInt64()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to increment key %v", r.Key)
	}

	if uint64(totalRequests) > r.Limit {
		return &Result{
			State:         Deny,
			TotalRequests: uint64(totalRequests),
			ExpiresAt:     expiresAt,
		}, nil
	}

	return &Result{
		State:         Allow,
		TotalRequests: uint64(totalRequests),
		ExpiresAt:     expiresAt,
	}, nil
}
