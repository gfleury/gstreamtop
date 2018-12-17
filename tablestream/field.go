package tablestream

type fieldType string

const (
	VARCHAR  fieldType = "varchar"
	INTEGER  fieldType = "integer"
	DATETIME fieldType = "datetime"
)

type Field struct {
	name      string
	fieldType fieldType
	table     Table
}
