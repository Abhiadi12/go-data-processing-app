package api

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// limiter is a token bucket per client IP. A client may send burst requests
// at once, then perSecond requests per second on average. Buckets that have
// refilled are dropped now and then, so the map does not grow forever.
type limiter struct {
	perSecond float64
	burst     float64
	now       func() time.Time

	mu        sync.Mutex
	clients   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

const sweepEvery = time.Minute

func newLimiter(perSecond, burst int, now func() time.Time) *limiter {
	return &limiter{
		perSecond: float64(perSecond),
		burst:     float64(burst),
		now:       now,
		clients:   make(map[string]*bucket),
		lastSweep: now(),
	}
}

// allow takes one token from key's bucket. With none left it returns false
// and how long until the next token arrives.
func (l *limiter) allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.sweep(now)

	b, ok := l.clients[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.clients[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.perSecond)
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.perSecond * float64(time.Second))
}

// sweep drops buckets that are full again: such a client is treated exactly
// like one never seen before, so forgetting it changes nothing.
func (l *limiter) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < sweepEvery {
		return
	}
	l.lastSweep = now
	refill := time.Duration(l.burst / l.perSecond * float64(time.Second))
	for key, b := range l.clients {
		if now.Sub(b.last) >= refill {
			delete(l.clients, key)
		}
	}
}

// rateLimit answers 429 with Retry-After when a client calls /api/ routes
// faster than allowed. /healthz and the docs are not limited, so health
// checks keep working. perSecond 0 turns it off.
func rateLimit(perSecond, burst int) middleware {
	return func(next http.Handler) http.Handler {
		if perSecond <= 0 {
			return next
		}
		l := newLimiter(perSecond, burst, time.Now)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				if ok, wait := l.allow(clientIP(r)); !ok {
					w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
					writeError(w, http.StatusTooManyRequests, "too many requests, try again later")
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// clientIP is the address the connection came from. X-Forwarded-For is not
// used: any client can set it to dodge the limit.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
