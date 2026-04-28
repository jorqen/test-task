package types

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string]*MsgQueue
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*MsgQueue),
	}
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	title := strings.Trim(r.URL.Path, "/")
	if title == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		b.handlePut(w, r, title)
	case http.MethodGet:
		b.handleGet(w, r, title)
	default:
		w.Header().Set("Allow", strings.Join([]string{http.MethodGet, http.MethodPut}, ", "))
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handlePut(w http.ResponseWriter, r *http.Request, title string) {
	const msgParam = "v"

	query := r.URL.Query()
	if !query.Has(msgParam) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	b.put(title, query.Get(msgParam))
	w.WriteHeader(http.StatusOK)
}

func (b *Broker) handleGet(w http.ResponseWriter, r *http.Request, title string) {
	timeout, ok := parseTimeout(r.URL.Query())
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	msg, ok := b.get(r.Context(), title, timeout)
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

	timeoutSeconds, err := strconv.ParseUint(query.Get(timeoutParam), 10, 64)
	if err != nil {
		return 0, false
	}

	return time.Duration(timeoutSeconds) * time.Second, true
}

func (b *Broker) put(title, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getOrCreateQueue(title)
	q.Push(msg)
	b.cleanup(q)
}

func (b *Broker) get(ctx context.Context, title string, timeout time.Duration) (string, bool) {
	msg, ok := b.getMessage(title)
	if ok {
		return msg, true
	}
	if timeout <= 0 {
		return "", false
	}

	return b.waitMessage(ctx, title, timeout)
}

func (b *Broker) getMessage(title string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if q := b.queues[title]; q != nil {
		msg, ok := q.PopMessage()
		if !ok {
			return "", false
		}
		b.cleanup(q)
		return msg, true
	}

	return "", false
}

func (b *Broker) waitMessage(ctx context.Context, title string, timeout time.Duration) (string, bool) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch, cancelWait := b.newWaiter(title)
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

func (b *Broker) newWaiter(title string) (<-chan string, func() bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getOrCreateQueue(title)
	if msg, ok := q.PopMessage(); ok {
		b.cleanup(q)
		ch := make(chan string, 1)
		ch <- msg
		return ch, func() bool { return false }
	}

	elem, ch := q.NewWaiter()
	return ch, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()

		q.RemoveWaiter(elem)
		b.cleanup(q)
		return true
	}
}

func (b *Broker) getOrCreateQueue(title string) *MsgQueue {
	q := b.queues[title]
	if q == nil {
		q = NewQueue(title)
		b.queues[title] = q
	}
	return q
}

func (b *Broker) cleanup(q *MsgQueue) {
	if q.Empty() && b.queues[q.Title()] == q {
		delete(b.queues, q.Title())
	}
}
