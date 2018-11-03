package tablestream

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/xwb1989/sqlparser"
)

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
	if len(t.fields) != len(stmt.TableSpec.Columns) || len(regexFields) != len(stmt.TableSpec.Columns) {
		return fmt.Errorf("regex groups doesn't match table columns: missing %d fields", len(regexFields)-len(stmt.TableSpec.Columns))
	}
	s.AddTable(t)
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

func (s *Stream) Query(query string) error {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		err = s.prepareSelect(stmt)
	case *sqlparser.DDL:
		err = s.prepareCreate(stmt)
	}
	return err
}

func (view *View) createFieldMapping(selectedExpr sqlparser.SelectExprs, table *Table, deep bool) string {

	for _, selectedExpr := range selectedExpr {

		switch selectedExpr := selectedExpr.(type) {
		case *sqlparser.StarExpr:
			// return any field as it is considering '*'
			if deep {
				fieldName := "*"
				field := table.Field(table.fields[0].name)

				view.AddViewData(&ViewData{
					field: field,
					name:  fieldName,
					data:  make(map[string]interface{}),
				})
				return "*"
			}

		case *sqlparser.AliasedExpr:
			switch selectedExpr := selectedExpr.Expr.(type) {
			case *sqlparser.ColName:
				fieldName := selectedExpr.Name.String()
				field := table.Field(fieldName)

				viewData := &ViewData{
					field: field,
					name:  fieldName,
					data:  make(map[string]interface{}),
				}
				viewData.UpdateModifier("SetValue")
				view.AddViewData(viewData)

				if deep {
					return fieldName
				}

			case *sqlparser.FuncExpr:
				modfier := selectedExpr.Name.String()
				fieldName := view.createFieldMapping(selectedExpr.Exprs, table, true)
				viewData := view.ViewData(fieldName)
				if viewData.name == "" {
					continue
				}
				viewData.UpdateModifier(modfier)
			}
		}
	}
	return ""
}
