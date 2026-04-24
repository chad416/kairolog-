package log

import "sync"

type Log struct {
	mu       sync.RWMutex
	messages []string
}

func New() *Log {
	return &Log{
		messages: make([]string, 0),
	}
}

func (l *Log) Append(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.messages = append(l.messages, message)
}

func (l *Log) ReadAll() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	messages := make([]string, len(l.messages))
	copy(messages, l.messages)

	return messages
}
