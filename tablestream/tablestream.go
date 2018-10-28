package tablestream

import (
	"regexp"
)

type Table struct {
	name          string
	fields        []*Field
	fieldRegexMap *regexp.Regexp
	rowSeparator  string
	typeInstance  map[string]chan map[string]string
	viewsIncluded []View
}

func CreateTable(name string) *Table {
	t := &Table{
		name:         name,
		typeInstance: make(map[string]chan map[string]string),
	}
	return t
}

func (t *Table) AddField(f *Field) error {
	t.fields = append(t.fields, f)
	return nil
}

func (t *Table) AddView(v View) error {
	t.viewsIncluded = append(t.viewsIncluded, v)
	return nil
}

func (t *Table) GetField(name string) *Field {
	for i, field := range t.fields {
		if field.name == name {
			return t.fields[i]
		}
	}
	return &Field{}
}

func (t *Table) AddRow(row string) error {
	match := t.fieldRegexMap.FindStringSubmatch(row)
	if len(match) > 0 {
		tableRow := make(map[string]string)
		for i, name := range t.fieldRegexMap.SubexpNames() {
			if i != 0 && name != "" {
				tableRow[name] = match[i]
			}
		}
		for _, fieldTypeInstanceChannel := range t.typeInstance {
			fieldTypeInstanceChannel <- tableRow
		}
	}
	return nil
}
