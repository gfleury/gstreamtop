package tablestream

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type TableJson struct {
	TableSimple
}

func CreateTableJson(name string) *TableJson {
	t := TableJson{}

	t.SetName(name)
	t.TableSimple.typeInstance = make(map[string]chan map[string]string)

	return &t
}

func (t *TableJson) AddRow(data interface{}) error {
	// row is a json line
	row := data.(string)
	tableRowInterface := make(map[string]interface{})
	// Assuming row is a JSON string, we can unmarshal it into tableRow
	if err := json.Unmarshal([]byte(row), &tableRowInterface); err != nil {
		fmt.Println("Error xxxxx JSON row: ", err)
		return nil
	}

	// Convert tableRowInterface members all to strings into tableRow
	tableRow := make(map[string]string)
	for k, v := range tableRowInterface {
		tableRow[k] = fmt.Sprintf("%v", v)
	}

	for _, fieldTypeInstanceChannel := range t.typeInstance {
		fieldTypeInstanceChannel <- tableRow
	}

	return nil
}

func jsonMapping(stmt *sqlparser.DDL) (t Table, fields int, err error) {
	t = CreateTableJson(stmt.NewName.Name.String())

	for _, column := range stmt.TableSpec.Columns {
		fieldName := column.Name.String()
		if strings.HasSuffix(fieldName, "x") {
			fieldName = strings.TrimSuffix(fieldName, "x")
		}
		err = t.AddField(&Field{
			name:      fieldName,
			fieldType: fieldType(column.Type.Type),
		})
	}

	fields = len(stmt.TableSpec.Columns)
	return
}
