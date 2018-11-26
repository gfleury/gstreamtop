package tablestream

import (
	"sync"
)

type TableSimple struct {
	Table
	name         string
	fields       []*Field
	rowSeparator string
	typeInstance map[string]chan map[string]string
	lock         sync.Mutex
}

func CreateTable(name string) *TableSimple {
	t := &TableSimple{
		name:         name,
		typeInstance: make(map[string]chan map[string]string),
	}
	return t
}

func (t *TableSimple) AddField(f *Field) error {
	t.fields = append(t.fields, f)
	return nil
}

func (t *TableSimple) Field(name string) *Field {
	for i, field := range t.fields {
		if field.name == name {
			return t.fields[i]
		}
	}
	return &Field{}
}

func (t *TableSimple) Fields() []*Field {
	return t.fields
}

func (t *TableSimple) RowSeparator() string {
	return t.rowSeparator
}

func (t *TableSimple) Lock() {
	t.lock.Lock()
}

func (t *TableSimple) Unlock() {
	t.lock.Unlock()
}

func (t *TableSimple) SetRowSeparator(sep string) {
	t.rowSeparator = sep
}

func (t *TableSimple) Name() string {
	return t.name
}

func (t *TableSimple) SetName(name string) {
	t.name = name
}

func (t *TableSimple) SetTypeInstance(key string, channel chan map[string]string) {
	t.typeInstance[key] = channel
}

func (t *TableSimple) TypeInstance(key string) chan map[string]string {
	return t.typeInstance[key]
}
