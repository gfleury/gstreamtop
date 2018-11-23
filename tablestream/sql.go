package tablestream

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (s *Stream) prepareCreate(stmt *sqlparser.DDL) (err error) {
	if !strings.Contains(stmt.TableSpec.Options, "FIELDS IDENTIFIED by ") {
		return fmt.Errorf("unable to find FIELDS IDENTIFIED by")
	}
	t := CreateTable(stmt.NewName.Name.String())

	// Handle FIELDS IDENTIFIED BY
	regexIdentifiedBy := regexp.MustCompile(`(?mi)IDENTIFIED BY (?P<regex>('([^']|\\"|\\')*.')|("([^"]|\\"|\\')*."))`)
	regexMap := regexIdentifiedBy.FindStringSubmatch(stmt.TableSpec.Options)
	if len(regexMap) < 2 {
		return fmt.Errorf("no FIELDS IDENTIFIED BY found")
	}
	regexMapTrimmed := strings.TrimPrefix(strings.TrimPrefix(regexMap[1], "'"), "\"")
	regexMapTrimmed = strings.TrimSuffix(strings.TrimSuffix(regexMapTrimmed, "'"), "\"")
	t.fieldRegexMap, err = regexp.Compile(regexMapTrimmed)
	if err != nil {
		return fmt.Errorf("regex present on FIELDS IDENTIFIED by failed to compile: %s", err.Error())
	}

	// Handle LINES TERMINATED BY
	linesTerminatedBy := regexp.MustCompile(`(?mi)LINES TERMINATED BY (?P<nr>('([^']|\\"|\\')*.')|("([^"]|\\"|\\')*."))`)
	crMap := linesTerminatedBy.FindStringSubmatch(stmt.TableSpec.Options)
	if len(crMap) > 1 {
		t.rowSeparator = strings.TrimPrefix(strings.TrimPrefix(regexMap[1], "'"), "\"")
		t.rowSeparator = strings.TrimSuffix(strings.TrimSuffix(t.rowSeparator, "'"), "\"")
	} else {
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

	// TODO Handle more than one GROUP BY
	if len(stmt.GroupBy) < 1 {
		return fmt.Errorf("the query should have at least one GROUP BY, for filtering use grep")
	}
	groupByStmt := stmt.GroupBy[0]
	groupByColumn, ok := groupByStmt.(*sqlparser.ColName)
	var groupBy string
	if !ok {
		groupByFunc, ok := groupByStmt.(*sqlparser.FuncExpr)
		if !ok {
			return fmt.Errorf("GROUP BY should be collumn name or function")
		}
		funcName := groupByFunc.Name.String()
		groupByColumn = groupByFunc.Exprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
		groupBy = fmt.Sprintf("%s(%s)", funcName, groupByColumn.Name.String())
	} else {
		groupBy = groupByColumn.Name.String()
	}

	view := CreateView(fmt.Sprintf("%v", stmt), groupBy)

	_, err = view.createFieldMapping(stmt.SelectExprs, table, false)
	if err != nil {
		return err
	}

	groupByFields := view.ViewDataByName(groupBy)
	if len(groupByFields) < 1 {
		return fmt.Errorf("GROUP BY column not found")
	}

	// TODO Handle more than one GROUP BY
	groupByField := groupByFields[0]
	view.groupByField = &ViewData{
		data:        make(map[string]interface{}),
		field:       groupByField.field,
		modifier:    groupByField.modifier,
		name:        groupByField.name,
		updateValue: groupByField.updateValue,
		varType:     groupByField.varType,
	}

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

func (view *View) createFieldMapping(selectedExpr sqlparser.SelectExprs, table *Table, deep bool) (fieldName string, err error) {

	for _, selectedExpr := range selectedExpr {

		switch selectedExpr := selectedExpr.(type) {
		case *sqlparser.StarExpr:
			// return any field as it is considering '*'
			if deep {
				fieldName = "*"
				field := table.Field(table.fields[0].name)

				view.AddViewData(&ViewData{
					field: field,
					name:  fieldName,
					data:  make(map[string]interface{}),
				})
				return fieldName, err
			}

		case *sqlparser.AliasedExpr:

			asFieldName := selectedExpr.As.String()

			switch selectedExpr := selectedExpr.Expr.(type) {
			case *sqlparser.ColName:
				fieldName := selectedExpr.Name.String()
				field := table.Field(fieldName)

				if asFieldName != "" {
					fieldName = asFieldName
				}

				viewData := &ViewData{
					field: field,
					name:  fieldName,
					data:  make(map[string]interface{}),
				}
				err = viewData.UpdateModifier("SetValue")
				view.AddViewData(viewData)

				if deep {
					return fieldName, err
				}

			case *sqlparser.FuncExpr:
				modfier := selectedExpr.Name.String()
				fieldName, err = view.createFieldMapping(selectedExpr.Exprs, table, true)
				viewData := view.ViewData(fieldName)
				if viewData.name == "" {
					continue
				}

				err = viewData.UpdateModifier(modfier)
				if err != nil {
					return fieldName, err
				}

				if asFieldName != "" {
					viewData.name = asFieldName
				} else {
					viewData.name = fmt.Sprintf("%s(%s)", modfier, fieldName)
				}

			}
		}
	}
	return fieldName, err
}
