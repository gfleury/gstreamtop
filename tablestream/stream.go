package tablestream

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type Stream struct {
	tables []*Table
	views  []*View
}

func (s *Stream) GetTable(name string) (*Table, error) {
	for i, table := range s.tables {
		if table.name == name {
			return s.tables[i], nil
		}
	}
	return nil, fmt.Errorf("not table named %s", name)
}

func (s *Stream) AddTable(t *Table) {
	s.tables = append(s.tables, t)
}

func (s *Stream) AddView(v *View) {
	s.views = append(s.views, v)
}

func (s *Stream) Query(query string) error {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		s.prepareSelect(stmt)
	case *sqlparser.DDL:
		s.prepareCreate(stmt)
	}

	return nil
}

func (s *Stream) prepareSelect(stmt *sqlparser.Select) error {
	table, err := s.GetTable(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
	if err != nil {
		return err
	}
	if len(stmt.GroupBy) < 1 {
		return fmt.Errorf("the query should have at least one GROUP BY, for filtering use grep")
	}
	groupBy := stmt.GroupBy[0].(*sqlparser.ColName).Name.String()
	view := CreateView(fmt.Sprintf("%v", stmt), groupBy)
	view.createFieldMapping(stmt.SelectExprs, table, false)
	view.AddTable(table)
	s.AddView(view)
	go view.UpdateView()
	return nil
}

func (s *Stream) prepareCreate(stmt *sqlparser.DDL) error {
	if !strings.Contains(stmt.TableSpec.Options, "FIELDS IDENTIFIED by ") {
		return fmt.Errorf("unable to find FIELDS IDENTIFIED by")
	}
	t := CreateTable(stmt.NewName.Name.String())
	optionsTokens := strings.Split(stmt.TableSpec.Options, " ")
	for i, token := range optionsTokens {
		switch strings.ToUpper(token) {
		case "FIELDS":
			var err error
			t.fieldRegexMap, err = regexp.Compile(strings.TrimSuffix(strings.TrimPrefix(optionsTokens[i+3], "'"), "'"))
			if err != nil {
				return fmt.Errorf("regex present on FIELDS IDENTIFIED by failed to compile: %s", err.Error())
			}

		case "LINES":
			t.rowSeparator = strings.TrimSuffix(strings.TrimPrefix(optionsTokens[i+3], "'"), "'")
		}
	}

	if t.rowSeparator == "" {
		t.rowSeparator = "\n"
	}

	regexFields := t.fieldRegexMap.SubexpNames()[1:]
	for _, column := range stmt.TableSpec.Columns {
		for _, field := range regexFields {
			if column.Name.String() == field {
				t.AddField(&Field{
					name:      field,
					fieldType: fieldType(column.Type.Type),
					table:     t,
				})
				break
			}
		}
	}
	if len(t.fields) != len(stmt.TableSpec.Columns) {
		return fmt.Errorf("regex groups doesn't match table columns: %v %v", t.fields, stmt.TableSpec.Columns)
	}
	s.AddTable(t)
	return nil
}
