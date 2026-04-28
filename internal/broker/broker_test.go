package broker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestBrokerPutGetFIFO(t *testing.T) {
	b := New()

	request(t, b, http.MethodPut, "/pet?v=cat", http.StatusOK, "")
	request(t, b, http.MethodPut, "/pet?v=dog", http.StatusOK, "")
	request(t, b, http.MethodPut, "/role?v=manager", http.StatusOK, "")

	request(t, b, http.MethodGet, "/pet", http.StatusOK, "cat")
	request(t, b, http.MethodGet, "/pet", http.StatusOK, "dog")
	request(t, b, http.MethodGet, "/pet", http.StatusNotFound, "")
	request(t, b, http.MethodGet, "/role", http.StatusOK, "manager")
}

func TestBrokerRejectsBadRequests(t *testing.T) {
	b := New()

	request(t, b, http.MethodPut, "/pet", http.StatusBadRequest, "")
	request(t, b, http.MethodGet, "/pet?timeout=bad", http.StatusBadRequest, "")
	request(t, b, http.MethodGet, "/pet?timeout=-1", http.StatusBadRequest, "")
	request(t, b, http.MethodGet, "/pet?timeout=9223372036854775808", http.StatusBadRequest, "")
	request(t, b, http.MethodPost, "/pet", http.StatusMethodNotAllowed, "")
	request(t, b, http.MethodGet, "/", http.StatusNotFound, "")
}

func TestGetWaitsForMessage(t *testing.T) {
	b := New()
	errs := make(chan error, 1)

	go func() {
		code, body := doRequest(b, http.MethodGet, "/pet?timeout=1")
		if code != http.StatusOK || body != "cat" {
			errs <- fmt.Errorf("GET /pet?timeout=1 = %d %q, want %d %q", code, body, http.StatusOK, "cat")
			return
		}
		errs <- nil
	}()

	waitForPendingGets(t, b, "pet", 1)
	request(t, b, http.MethodPut, "/pet?v=cat", http.StatusOK, "")

	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestWaitingConsumersReceiveMessagesInRequestOrder(t *testing.T) {
	b := New()
	first := make(chan string, 1)
	second := make(chan string, 1)

	var wg sync.WaitGroup
	wg.Add(2)
	go waitGet(t, &wg, b, "/pet?timeout=1", first)
	waitForPendingGets(t, b, "pet", 1)

	go waitGet(t, &wg, b, "/pet?timeout=1", second)
	waitForPendingGets(t, b, "pet", 2)

	request(t, b, http.MethodPut, "/pet?v=cat", http.StatusOK, "")
	request(t, b, http.MethodPut, "/pet?v=dog", http.StatusOK, "")
	wg.Wait()

	if got := <-first; got != "cat" {
		t.Fatalf("first waiting GET received %q, want %q", got, "cat")
	}
	if got := <-second; got != "dog" {
		t.Fatalf("second waiting GET received %q, want %q", got, "dog")
	}
}

func TestTimeoutRemovesPendingGet(t *testing.T) {
	b := New()

	request(t, b, http.MethodGet, "/pet?timeout=0.01", http.StatusNotFound, "")
	request(t, b, http.MethodPut, "/pet?v=cat", http.StatusOK, "")
	request(t, b, http.MethodGet, "/pet", http.StatusOK, "cat")
}

func TestDeliveredMessageWinsOverLateCancel(t *testing.T) {
	b := New()
	ch, cancelWait := b.registerPendingGet("pet")

	b.put("pet", "cat")
	if cancelWait() {
		t.Fatal("cancelWait returned true after the message had already been delivered")
	}

	select {
	case msg := <-ch:
		if msg != "cat" {
			t.Fatalf("message = %q, want %q", msg, "cat")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivered message")
	}
}

func request(t *testing.T, h http.Handler, method, target string, wantCode int, wantBody string) {
	t.Helper()

	code, body := doRequest(h, method, target)
	if code != wantCode || body != wantBody {
		t.Fatalf("%s %s = %d %q, want %d %q", method, target, code, body, wantCode, wantBody)
	}
}

func doRequest(h http.Handler, method, target string) (int, string) {
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}

func waitGet(t *testing.T, wg *sync.WaitGroup, h http.Handler, target string, results chan<- string) {
	t.Helper()
	defer wg.Done()

	code, body := doRequest(h, http.MethodGet, target)
	if code != http.StatusOK {
		t.Errorf("GET %s = %d %q, want %d", target, code, body, http.StatusOK)
	}
	results <- body
}

func waitForPendingGets(t *testing.T, b *Broker, title string, count int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		b.mu.Lock()
		got := 0
		if q := b.queues[title]; q != nil {
			got = q.pendingGets.Len()
		}
		b.mu.Unlock()

		if got == count {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("pending GET count for %q = %d, want %d", title, got, count)
		case <-ticker.C:
		}
	}
}
