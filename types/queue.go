package types

import (
	"container/list"
)

type MsgQueue struct {
	title   string
	msgs    list.List
	waiters list.List
}

func NewQueue(title string) *MsgQueue {
	return &MsgQueue{title: title}
}

func (mq *MsgQueue) Title() string {
	if mq == nil {
		return ""
	}
	return mq.title
}

func (mq *MsgQueue) Push(msg string) {
	if elem := mq.waiters.Front(); elem != nil {
		mq.waiters.Remove(elem).(chan string) <- msg
		return
	}

	mq.msgs.PushBack(msg)
}

func (mq *MsgQueue) PopMessage() (string, bool) {
	if mq == nil {
		return "", false
	}

	elem := mq.msgs.Front()
	if elem == nil {
		return "", false
	}
	return mq.msgs.Remove(elem).(string), true
}

func (mq *MsgQueue) Empty() bool {
	return mq == nil || mq.msgs.Len() == 0 && mq.waiters.Len() == 0
}

func (mq *MsgQueue) NewWaiter() (*list.Element, <-chan string) {
	ch := make(chan string, 1)
	return mq.waiters.PushBack(ch), ch
}

func (mq *MsgQueue) RemoveWaiter(elem *list.Element) {
	mq.waiters.Remove(elem)
}
