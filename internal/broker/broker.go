package broker

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func New() *Broker {
	return &Broker{
		queues: make(map[string]*queue),
	}
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(r.URL.Path, "/")
	if name == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		b.handlePut(w, r, name)
	case http.MethodGet:
		b.handleGet(w, r, name)
	default:
		w.Header().Set("Allow", strings.Join([]string{http.MethodGet, http.MethodPut}, ", "))
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handlePut(w http.ResponseWriter, r *http.Request, name string) {
	const msgParam = "v"

	query := r.URL.Query()
	if !query.Has(msgParam) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	b.put(name, query.Get(msgParam))
	w.WriteHeader(http.StatusOK)
}

func (b *Broker) handleGet(w http.ResponseWriter, r *http.Request, name string) {
	timeout, ok := parseTimeout(r.URL.Query())
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	msg, ok := b.get(r.Context(), name, timeout)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(msg))
}

func parseTimeout(query url.Values) (time.Duration, bool) {
	const timeoutParam = "timeout"
	if !query.Has(timeoutParam) {
		return 0, true
	}

	timeout, err := time.ParseDuration(query.Get(timeoutParam) + "s")
	if err != nil || timeout < 0 {
		return 0, false
	}

	return timeout, true
}

func (b *Broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getOrCreateQueue(name)
	q.push(msg)
	b.cleanup(q)
}

func (b *Broker) get(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	msg, ok := b.getMessage(name)
	if ok {
		return msg, true
	}
	if timeout <= 0 {
		return "", false
	}

	return b.waitMessage(ctx, name, timeout)
}

func (b *Broker) getMessage(name string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if q := b.queues[name]; q != nil {
		msg, ok := q.pop()
		if !ok {
			return "", false
		}
		b.cleanup(q)
		return msg, true
	}

	return "", false
}

func (b *Broker) waitMessage(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch, cancelWait := b.registerPendingGet(name)
	return wait(waitCtx, ch, cancelWait)
}

func wait(ctx context.Context, ch <-chan string, cancelWait func() bool) (string, bool) {
	select {
	case msg := <-ch:
		return msg, true
	case <-ctx.Done():
	}

	if cancelWait() {
		return "", false
	}

	select {
	case msg := <-ch:
		return msg, true
	default:
		return "", false
	}
}

func (b *Broker) registerPendingGet(name string) (<-chan string, func() bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getOrCreateQueue(name)
	if msg, ok := q.pop(); ok {
		b.cleanup(q)
		ch := make(chan string, 1)
		ch <- msg
		return ch, func() bool { return false }
	}

	pending := q.addPendingGet()
	return pending.ch, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()

		if !q.cancelPendingGet(pending) {
			return false
		}
		b.cleanup(q)
		return true
	}
}

func (b *Broker) getOrCreateQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = newQueue(name)
		b.queues[name] = q
	}
	return q
}

func (b *Broker) cleanup(q *queue) {
	if q.empty() && b.queues[q.name] == q {
		delete(b.queues, q.name)
	}
}
