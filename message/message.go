package message

type Message[T any] struct {
	deleteTag string
	Body      T `json:"body"`
}

func (m *Message[T]) DeleteTag() string {
	return m.deleteTag
}

func (m *Message[T]) SetDeleteTag(tag string) {
	m.deleteTag = tag
}
