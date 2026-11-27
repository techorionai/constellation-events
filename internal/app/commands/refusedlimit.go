package commands

import (
	"container/list"
	"sync"
	"time"
)

const (
	// RefusedWindow is the length of the window of the refused limit.
	RefusedWindow = time.Minute
	// MaxRefusedPrincipals is the largest number of principals that the
	// limiter keeps in memory.
	MaxRefusedPrincipals = 10000
)

// RefusedLimiter limits the audit entries of refused registrations for each
// principal. It keeps a fixed window of one minute for each principal ID. When
// it keeps more than MaxRefusedPrincipals windows, it removes the oldest
// window first. The limiter lives in the process.
type RefusedLimiter struct {
	limit int
	now   func() time.Time

	mu      sync.Mutex
	windows map[string]*refusedWindow
	order   *list.List // windows by start time, oldest first
}

type refusedWindow struct {
	key   string
	start time.Time
	count int
	elem  *list.Element
}

// NewRefusedLimiter makes a limiter. limit is the number of entries for each
// principal in each window. A nil now means time.Now.
func NewRefusedLimiter(limit int, now func() time.Time) *RefusedLimiter {
	if now == nil {
		now = time.Now
	}
	return &RefusedLimiter{limit: limit, now: now, windows: map[string]*refusedWindow{}, order: list.New()}
}

// Allow tells whether the principal can write one more entry in the window,
// and counts the entry when it can.
func (l *RefusedLimiter) Allow(principalID string) bool {
	t := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w, ok := l.windows[principalID]
	switch {
	case !ok:
		w = &refusedWindow{key: principalID, start: t}
		w.elem = l.order.PushBack(w)
		l.windows[principalID] = w
		l.evict(t)
	case t.Sub(w.start) >= RefusedWindow:
		w.start, w.count = t, 0
		l.order.MoveToBack(w.elem)
	}
	if w.count >= l.limit {
		return false
	}
	w.count++
	return true
}

// evict removes the windows that ended, and then the oldest windows while the
// limiter has too many.
func (l *RefusedLimiter) evict(t time.Time) {
	for l.order.Len() > 0 {
		front := l.order.Front()
		w := front.Value.(*refusedWindow)
		if l.order.Len() <= MaxRefusedPrincipals && t.Sub(w.start) < RefusedWindow {
			return
		}
		l.order.Remove(front)
		delete(l.windows, w.key)
	}
}
