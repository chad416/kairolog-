package log

import (
	"reflect"
	"testing"
)

func TestAppendAddsMessage(t *testing.T) {
	log := New()

	log.Append("hello")

	messages := log.ReadAll()
	expected := []string{"hello"}

	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func TestReadAllReturnsAllMessages(t *testing.T) {
	log := New()

	log.Append("first")
	log.Append("second")

	messages := log.ReadAll()
	expected := []string{"first", "second"}

	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func TestMultipleAppends(t *testing.T) {
	log := New()

	log.Append("one")
	log.Append("two")
	log.Append("three")

	messages := log.ReadAll()
	expected := []string{"one", "two", "three"}

	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func TestReadAllReturnsCopy(t *testing.T) {
	log := New()

	log.Append("original")

	messages := log.ReadAll()
	messages[0] = "changed"

	actual := log.ReadAll()
	expected := []string{"original"}

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
