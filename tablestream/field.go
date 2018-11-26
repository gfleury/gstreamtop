package tablestream

type fieldType string

const (
	VARCHAR fieldType = "varchar"
	INTEGER fieldType = "integer"
)

type Field struct {
	name      string
	fieldType fieldType
	table     Table
}
