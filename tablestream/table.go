package tablestream

type Table interface {
	AddRow(interface{}) error
	SetRowSeparator(string)
	RowSeparator() string
	AddField(*Field) error
	Fields() []*Field
	Field(name string) *Field
	Name() string
	SetName(string)
	SetTypeInstance(string, chan map[string]string)
	TypeInstance(string) chan map[string]string
	Lock()
	Unlock()
}
