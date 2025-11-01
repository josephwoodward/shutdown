package shutdown

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func assertOpen(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
		t.Error("expected channel to be open")
	default:
	}
}

func assertClosed(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Millisecond * 100):
		t.Error("expected channel to be closed")
	}
}

func TestSignallerNotClosed(t *testing.T) {
	s := NewSignaller()

	assertOpen(t, s.SoftStopChan())
	assert.False(t, s.IsSoftStopSignalled())

	assertOpen(t, s.HardStopChan())
	assert.False(t, s.IsHardStopSignalled())

	assertOpen(t, s.HasStoppedChan())
	assert.False(t, s.IsHasStoppedSignalled())

	s.TriggerHasStopped()

	assertOpen(t, s.SoftStopChan())
	assert.False(t, s.IsSoftStopSignalled())

	assertOpen(t, s.HardStopChan())
	assert.False(t, s.IsHardStopSignalled())

	assertClosed(t, s.HasStoppedChan())
	assert.True(t, s.IsHasStoppedSignalled())
}

func TestSignallerAtLeisure(t *testing.T) {
	s := NewSignaller()
	s.TriggerSoftStop()

	assertClosed(t, s.SoftStopChan())
	assert.True(t, s.IsSoftStopSignalled())

	assertOpen(t, s.HardStopChan())
	assert.False(t, s.IsHardStopSignalled())

	assertOpen(t, s.HasStoppedChan())
	assert.False(t, s.IsHasStoppedSignalled())

	s.TriggerHasStopped()

	assertClosed(t, s.SoftStopChan())
	assert.True(t, s.IsSoftStopSignalled())

	assertOpen(t, s.HardStopChan())
	assert.False(t, s.IsHardStopSignalled())

	assertClosed(t, s.HasStoppedChan())
	assert.True(t, s.IsHasStoppedSignalled())
}

func TestSignallerNow(t *testing.T) {
	s := NewSignaller()
	s.TriggerHardStop()

	assertClosed(t, s.SoftStopChan())
	assert.True(t, s.IsSoftStopSignalled())

	assertClosed(t, s.HardStopChan())
	assert.True(t, s.IsHardStopSignalled())

	assertOpen(t, s.HasStoppedChan())
	assert.False(t, s.IsHasStoppedSignalled())

	s.TriggerHasStopped()

	assertClosed(t, s.SoftStopChan())
	assert.True(t, s.IsSoftStopSignalled())

	assertClosed(t, s.HardStopChan())
	assert.True(t, s.IsHardStopSignalled())

	assertClosed(t, s.HasStoppedChan())
	assert.True(t, s.IsHasStoppedSignalled())
}

func TestSignallerAtLeisureCtx(t *testing.T) {
	s := NewSignaller()

	// Cancelled from original context
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done := s.SoftStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	inDone()
	assertClosed(t, ctx.Done())
	done()

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.SoftStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at leisure signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.SoftStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerSoftStop()
	assertClosed(t, ctx.Done())
	done()
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancel(context.Background())
	s = NewSignaller()
	ctx, done = s.SoftStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerHardStop()
	assertClosed(t, ctx.Done())
	done()
	inDone()
}

func TestSignallerNowCtx(t *testing.T) {
	s := NewSignaller()

	// Cancelled from original context
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done := s.HardStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	inDone()
	assertClosed(t, ctx.Done())
	done()

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HardStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Not cancelled from at leisure signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HardStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerSoftStop()
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HardStopCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerHardStop()
	assertClosed(t, ctx.Done())
	done()
	inDone()
}

func TestSignallerHasClosedCtx(t *testing.T) {
	s := NewSignaller()

	// Cancelled from original context
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done := s.HasStoppedCtx(inCtx)
	assertOpen(t, ctx.Done())
	inDone()
	assertClosed(t, ctx.Done())
	done()

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HasStoppedCtx(inCtx)
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at leisure signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HasStoppedCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerHasStopped()
	assertClosed(t, ctx.Done())
	done()
	inDone()
}

func TestSignallerAtLeisureCtxWithCause(t *testing.T) {
	s := NewSignaller()
	customErr := errors.New("custom error")

	// Cancelled from original context
	inCtx, inDoneWithCause := context.WithCancelCause(context.Background())
	ctx, done := s.SoftStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	inDoneWithCause(customErr)
	assertClosed(t, ctx.Done())
	assert.Equal(t, customErr, context.Cause(ctx))
	done(nil)

	// Cancelled from returned cancel func
	inCtx, inDoneWithCause = context.WithCancelCause(context.Background())
	ctx, done = s.SoftStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	done(customErr)
	assertClosed(t, ctx.Done())
	assert.Equal(t, customErr, context.Cause(ctx))
	inDoneWithCause(nil)

	// Cancelled from at leisure signal
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done = s.SoftStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerSoftStop()
	assertClosed(t, ctx.Done())
	assert.Equal(t, ErrSoftStopSignalled, context.Cause(ctx))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.ErrorIs(t, context.Cause(ctx), context.Canceled)
	assert.ErrorIs(t, context.Cause(ctx), ErrSoftStopSignalled)
	done(nil)
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDoneWithCause = context.WithCancelCause(context.Background())
	s = NewSignaller()
	ctx, done = s.SoftStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerHardStop()
	assert.True(t, s.IsHardStopSignalled())
	assertClosed(t, ctx.Done())
	assert.Equal(t, ErrHardStopSignalled, context.Cause(ctx))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.ErrorIs(t, context.Cause(ctx), context.Canceled)
	assert.ErrorIs(t, context.Cause(ctx), ErrHardStopSignalled)
	done(nil)
	inDoneWithCause(nil)
}

func TestSignallerNowCtxWithCause(t *testing.T) {
	s := NewSignaller()
	customErr := errors.New("custom error")

	// Cancelled from original context
	inCtx, inDone := context.WithCancelCause(context.Background())
	ctx, done := s.HardStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	inDone(customErr)
	assertClosed(t, ctx.Done())
	assert.Equal(t, customErr, context.Cause(ctx))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	done(nil)

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancelCause(context.Background())
	ctx, done = s.HardStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	done(customErr)
	assertClosed(t, ctx.Done())
	assert.Equal(t, customErr, context.Cause(ctx))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	inDone(nil)

	// Not cancelled from at leisure signal
	inCtx, inDone = context.WithCancelCause(context.Background())
	ctx, done = s.HardStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerSoftStop()
	assertOpen(t, ctx.Done())
	done(customErr)
	assertClosed(t, ctx.Done())
	assert.Equal(t, customErr, context.Cause(ctx))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.NotEqual(t, ErrSoftStopSignalled, context.Cause(ctx))
	inDone(nil)

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancelCause(context.Background())
	ctx, done = s.HardStopCtxWithCause(inCtx)
	assertOpen(t, ctx.Done())
	s.TriggerHardStop()
	assertClosed(t, ctx.Done())
	assert.Equal(t, ErrHardStopSignalled, context.Cause(ctx))
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.ErrorIs(t, context.Cause(ctx), context.Canceled)
	done(nil)
	inDone(nil)
}
