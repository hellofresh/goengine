package mongodb

import (
	"context"
	"time"
)

// ContextStrategy is an interface that represents strategy for providing contexts for corresponding MongoDB EventStore
// instance queries to MongoDB
type ContextStrategy interface {
	Append() (context.Context, context.CancelFunc)
	GetEventsFor() (context.Context, context.CancelFunc)
	FromVersion() (context.Context, context.CancelFunc)
	CountEventsFor() (context.Context, context.CancelFunc)
	CreateIndices() (context.Context, context.CancelFunc)
}

// BackgroundContextStrategy is the ContextStrategy implementation that always returns Background context and noop cancel
type BackgroundContextStrategy struct {
	ctx context.Context
}

// NewBackgroundContextStrategy instantiates new BackgroundContextStrategy
func NewBackgroundContextStrategy() *BackgroundContextStrategy {
	return &BackgroundContextStrategy{ctx: context.Background()}
}

// Append is the ContextStrategy.Append() implementation
func (s *BackgroundContextStrategy) Append() (context.Context, context.CancelFunc) {
	return s.ctx, func() {}
}

// GetEventsFor is the ContextStrategy.GetEventsFor() implementation
func (s *BackgroundContextStrategy) GetEventsFor() (context.Context, context.CancelFunc) {
	return s.ctx, func() {}
}

// FromVersion is the ContextStrategy.FromVersion() implementation
func (s *BackgroundContextStrategy) FromVersion() (context.Context, context.CancelFunc) {
	return s.ctx, func() {}
}

// CountEventsFor is the ContextStrategy.CountEventsFor() implementation
func (s *BackgroundContextStrategy) CountEventsFor() (context.Context, context.CancelFunc) {
	return s.ctx, func() {}
}

// CreateIndices is the ContextStrategy.CreateIndices() implementation
func (s *BackgroundContextStrategy) CreateIndices() (context.Context, context.CancelFunc) {
	return s.ctx, func() {}
}

// TimeoutContextStrategy is the ContextStrategy implementation that returns configurable WithTimeout context and its cancel
type TimeoutContextStrategy struct {
	append         time.Duration
	getEventsFor   time.Duration
	fromVersion    time.Duration
	countEventsFor time.Duration
	createIndices  time.Duration
}

// TimeoutContextStrategyOption is the options type to configure TimeoutContextStrategy creation
type TimeoutContextStrategyOption func(s *TimeoutContextStrategy)

// NewTimeoutContextStrategy instantiates new TimeoutContextStrategy
func NewTimeoutContextStrategy(options ...TimeoutContextStrategyOption) *TimeoutContextStrategy {
	s := &TimeoutContextStrategy{
		append:         5 * time.Second,
		getEventsFor:   30 * time.Second,
		fromVersion:    30 * time.Second,
		countEventsFor: 5 * time.Second,
		createIndices:  5 * time.Second,
	}

	for _, o := range options {
		o(s)
	}

	return s
}

// Append is the ContextStrategy.Append() implementation
func (s *TimeoutContextStrategy) Append() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.append)
}

// GetEventsFor is the ContextStrategy.GetEventsFor() implementation
func (s *TimeoutContextStrategy) GetEventsFor() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.getEventsFor)
}

// FromVersion is the ContextStrategy.FromVersion() implementation
func (s *TimeoutContextStrategy) FromVersion() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.fromVersion)
}

// CountEventsFor is the ContextStrategy.CountEventsFor() implementation
func (s *TimeoutContextStrategy) CountEventsFor() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.countEventsFor)
}

// CreateIndices is the ContextStrategy.CreateIndices() implementation
func (s *TimeoutContextStrategy) CreateIndices() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.createIndices)
}

// NewAppendTimeout is the TimeoutContextStrategy configuration option to set a timeout for Append call
func NewAppendTimeout(timeout time.Duration) TimeoutContextStrategyOption {
	return func(s *TimeoutContextStrategy) {
		s.append = timeout
	}
}

// NewGetEventsForTimeout is the TimeoutContextStrategy configuration option to set a timeout for GetEventsFor call
func NewGetEventsForTimeout(timeout time.Duration) TimeoutContextStrategyOption {
	return func(s *TimeoutContextStrategy) {
		s.getEventsFor = timeout
	}
}

// NewFromVersionTimeout is the TimeoutContextStrategy configuration option to set a timeout for FromVersion call
func NewFromVersionTimeout(timeout time.Duration) TimeoutContextStrategyOption {
	return func(s *TimeoutContextStrategy) {
		s.fromVersion = timeout
	}
}

// NewCountEventsForTimeout is the TimeoutContextStrategy configuration option to set a timeout for CountEventsFor call
func NewCountEventsForTimeout(timeout time.Duration) TimeoutContextStrategyOption {
	return func(s *TimeoutContextStrategy) {
		s.countEventsFor = timeout
	}
}

// NewCreateIndicesTimeout is the TimeoutContextStrategy configuration option to set a timeout for CreateIndices call
func NewCreateIndicesTimeout(timeout time.Duration) TimeoutContextStrategyOption {
	return func(s *TimeoutContextStrategy) {
		s.createIndices = timeout
	}
}
