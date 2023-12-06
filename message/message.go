package message

type Message[T any] struct {
	deleteTag string
	Body      T
}

func (m *Message[T]) DeleteTag() string {
	return m.deleteTag
}
