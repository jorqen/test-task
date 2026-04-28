package broker

import (
	"container/list"
)

type queue struct {
	name        string
	msgs        list.List
	pendingGets list.List
}

type pendingGet struct {
	ch   chan string
	elem *list.Element
}

func newQueue(name string) *queue {
	return &queue{name: name}
}

func (q *queue) push(msg string) {
	if elem := q.pendingGets.Front(); elem != nil {
		pending := elem.Value.(*pendingGet)
		q.pendingGets.Remove(elem)
		pending.elem = nil
		pending.ch <- msg
		return
	}

	q.msgs.PushBack(msg)
}

func (q *queue) pop() (string, bool) {
	if q == nil {
		return "", false
	}

	elem := q.msgs.Front()
	if elem == nil {
		return "", false
	}
	return q.msgs.Remove(elem).(string), true
}

func (q *queue) empty() bool {
	return q == nil || q.msgs.Len() == 0 && q.pendingGets.Len() == 0
}

func (q *queue) addPendingGet() *pendingGet {
	pending := &pendingGet{ch: make(chan string, 1)}
	pending.elem = q.pendingGets.PushBack(pending)
	return pending
}

func (q *queue) cancelPendingGet(pending *pendingGet) bool {
	if pending.elem == nil {
		return false
	}
	q.pendingGets.Remove(pending.elem)
	pending.elem = nil
	return true
}
