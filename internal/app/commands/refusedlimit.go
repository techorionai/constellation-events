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
	// dropLogged is true after the first refused request of the window.
	dropLogged bool
	elem       *list.Element
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
	allowed, _ := l.Admit(principalID)
	return allowed
}

// Admit is like Allow. It also tells whether this call is the first refused
// call of the principal in the window, so that the caller can log it once.
func (l *RefusedLimiter) Admit(principalID string) (allowed, firstDrop bool) {
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
		w.start, w.count, w.dropLogged = t, 0, false
		l.order.MoveToBack(w.elem)
	}
	if w.count >= l.limit {
		first := !w.dropLogged
		w.dropLogged = true
		return false, first
	}
	w.count++
	return true, false
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
