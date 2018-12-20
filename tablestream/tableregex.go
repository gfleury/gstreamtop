package tablestream

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type TableRegex struct {
	TableSimple
	fieldRegexMap *regexp.Regexp
}

func CreateTableRegex(name string) *TableRegex {
	t := TableRegex{}

	t.SetName(name)
	t.TableSimple.typeInstance = make(map[string]chan map[string]string)

	return &t
}

func (t *TableRegex) AddRow(data interface{}) error {
	row := data.(string)
	match := t.fieldRegexMap.FindStringSubmatch(row)
	if len(match) > 0 {
		tableRow := make(map[string]string)
		for i, name := range t.fieldRegexMap.SubexpNames() {
			if i != 0 && name != "" {
				tableRow[name] = match[i]
			} else if name == "" {
				tableRow[t.fields[i].name] = match[i]
			}
		}
		for _, fieldTypeInstanceChannel := range t.typeInstance {
			fieldTypeInstanceChannel <- tableRow
		}
	}
	return nil
}

func regexMapping(stmt *sqlparser.DDL) (t Table, fields int, err error) {
	table := CreateTableRegex(stmt.NewName.Name.String())

	// Handle FIELDS IDENTIFIED BY
	regexIdentifiedBy := regexp.MustCompile(`(?mi)IDENTIFIED BY (?P<regex>('([^']|\\"|\\')*.')|("([^"]|\\"|\\')*."))`)
	regexMap := regexIdentifiedBy.FindStringSubmatch(stmt.TableSpec.Options)
	if len(regexMap) < 2 {
		return t, fields, fmt.Errorf("no FIELDS IDENTIFIED BY found")
	}
	regexMapTrimmed := strings.TrimPrefix(strings.TrimPrefix(regexMap[1], "'"), "\"")
	regexMapTrimmed = strings.TrimSuffix(strings.TrimSuffix(regexMapTrimmed, "'"), "\"")
	table.fieldRegexMap, err = regexp.Compile(regexMapTrimmed)
	if err != nil {
		return t, fields, fmt.Errorf("regex present on FIELDS IDENTIFIED by failed to compile: %s", err.Error())
	}

	regexFields := table.fieldRegexMap.SubexpNames()[1:]
	for _, column := range stmt.TableSpec.Columns {
		for _, field := range regexFields {
			if column.Name.String() == field {
				table.AddField(&Field{
					name:      field,
					fieldType: fieldType(column.Type.Type),
				})
				break
			}
		}
	}
	fields = len(regexFields)

	return table, fields, nil
}
