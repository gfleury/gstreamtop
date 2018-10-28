package tablestream

type fieldType string

const (
	VARCHAR fieldType = "varchar"
	INTEGER fieldType = "integer"
)

type Field struct {
	name      string
	fieldType fieldType
	errors    []error
	table     *Table
}

func (f *Field) AddError(err error) {
	f.errors = append(f.errors, err)
}
