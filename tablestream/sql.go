package tablestream

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (s *Stream) prepareCreate(stmt *sqlparser.DDL) (err error) {
	var t Table
	var fields int

	if strings.Contains(stmt.TableSpec.Options, "FIELDS IDENTIFIED by") {
		t, fields, err = regexMapping(stmt)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unable to find FIELDS IDENTIFIED by")
	}

	// Handle LINES TERMINATED BY
	linesTerminatedBy := regexp.MustCompile(`(?mi)LINES TERMINATED BY (?P<nr>('([^']|\\"|\\')*.')|("([^"]|\\"|\\')*."))`)
	crMap := linesTerminatedBy.FindStringSubmatch(stmt.TableSpec.Options)
	if len(crMap) > 1 {
		sep := strings.TrimPrefix(strings.TrimPrefix(crMap[1], "'"), "\"")
		sep = strings.TrimSuffix(strings.TrimSuffix(sep, "'"), "\"")
		t.SetRowSeparator(sep)
	} else {
		t.SetRowSeparator("\n")
	}

	if len(t.Fields()) != len(stmt.TableSpec.Columns) || fields != len(stmt.TableSpec.Columns) {
		return fmt.Errorf("regex groups doesn't match table columns: missing %d fields", fields-len(stmt.TableSpec.Columns))
	}
	s.AddTable(t)
	return nil
}

func (s *Stream) prepareSelect(stmt *sqlparser.Select) error {
	table, err := s.GetTable(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
	if err != nil {
		return err
	}
	// Handle LIMIT
	limit := 0
	if stmt.Limit != nil {
		limit, err = getLimit(stmt.Limit)
		if err != nil {
			return err
		}
	}

	// TODO Handle more than one column in GROUP BY
	if len(stmt.GroupBy) < 1 {
		return fmt.Errorf("the query should have at least one GROUP BY, for filtering use grep")
	}
	groupBy, _, err := getFieldByStmt(stmt.GroupBy[0])
	if err != nil {
		return err
	}

	// TODO Handle more than one column in ORDER BY
	var orderBy, direction string
	if len(stmt.OrderBy) > 0 {
		orderBy, direction, err = getFieldByStmt(stmt.OrderBy[0])
		if err != nil {
			return err
		}
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	stmt.Format(buf)
	view := CreateView(buf.String())

	_, err = view.createFieldMapping(stmt.SelectExprs, table, false)
	if err != nil {
		return err
	}

	groupByFields := view.ViewDataByName(groupBy)
	if len(groupByFields) < 1 {
		return fmt.Errorf("GROUP BY column %s not found", groupBy)
	}

	if orderBy != "" {
		orderByFields := view.ViewDataByName(orderBy)
		if len(orderByFields) < 1 {
			return fmt.Errorf("ORDER BY column %s not found", orderBy)
		}
		view.SetOrderBy(orderByFields, direction)
	}

	view.SetGroupBy(groupByFields)
	view.SetLimit(limit)

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

func (view *View) createFieldMapping(selectedExpr sqlparser.SelectExprs, table Table, deep bool) (fieldName string, err error) {

	for _, selectedExpr := range selectedExpr {

		switch selectedExpr := selectedExpr.(type) {
		case *sqlparser.StarExpr:
			// return any field as it is considering '*'
			if deep {
				fieldName = "*"
				field := table.Field(table.Fields()[0].name)

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

func getFieldByStmt(stmt interface{}) (fieldName, extra string, _ error) {
	var columnName string
	column, ok := stmt.(*sqlparser.ColName)
	if !ok {
		funcExpr, ok := stmt.(*sqlparser.FuncExpr)
		if !ok {
			orderExpr, ok := stmt.(*sqlparser.Order)
			if !ok {
				return fieldName, extra, fmt.Errorf("ORDER BY should be collumn name or function")
			}
			extra = orderExpr.Direction
			fieldName, _, err := getFieldByStmt(orderExpr.Expr)
			return fieldName, extra, err
		}
		funcName := funcExpr.Name.String()
		aliasedColumn, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			_, ok := funcExpr.Exprs[0].(*sqlparser.StarExpr)
			if !ok {
				return fieldName, extra, fmt.Errorf("ORDER BY should be EXISTENT collumn name or function")
			}
			columnName = "*"
		} else {
			column = aliasedColumn.Expr.(*sqlparser.ColName)
			columnName = column.Name.String()
		}
		fieldName = fmt.Sprintf("%s(%s)", funcName, columnName)
	} else {
		fieldName = column.Name.String()
	}
	return fieldName, extra, nil
}

func getLimit(stmt interface{}) (limit int, err error) {
	limitStmt, ok := stmt.(*sqlparser.Limit)
	if ok {
		limitValue, ok := limitStmt.Rowcount.(*sqlparser.SQLVal)
		if ok {
			return fromSQLValToInt(limitValue)
		}
	}
	return limit, fmt.Errorf("No LIMIT FOUND")
}

func fromSQLValToInt(sqlVal *sqlparser.SQLVal) (limit int, err error) {
	limitStr := string(sqlVal.Val)
	limit, err = strconv.Atoi(limitStr)
	return limit, err
}
