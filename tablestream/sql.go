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
	table, err := s.Table(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
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

	// Handle multiples GROUP BY
	if len(stmt.GroupBy) < 1 {
		return fmt.Errorf("the query should have at least one GROUP BY, for filtering use grep")
	}
	groupBy := make([]string, len(stmt.GroupBy))
	for i := range stmt.GroupBy {
		groupBy[i], _, _, _, err = getFieldByStmt(stmt.GroupBy[i])
		if err != nil {
			return err
		}
	}

	// Hanle multiples ORDER BY
	direction := make([]string, len(stmt.OrderBy))
	orderBy := make([]string, len(stmt.OrderBy))
	for i := range stmt.OrderBy {
		orderBy[i], direction[i], _, _, err = getFieldByStmt(stmt.OrderBy[i])
		if err != nil {
			return err
		}
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	stmt.Format(buf)
	view := CreateView(buf.String())

	_, _, err = view.createFieldMapping(stmt.SelectExprs, table, false)
	if err != nil {
		return err
	}

	var groupByFields []ViewData
	for i := range groupBy {
		groupByField := view.ViewDataByName(groupBy[i])
		if len(groupByField) < 1 {
			return fmt.Errorf("GROUP BY column %s not found", groupBy[i])
		}
		groupByFields = append(groupByFields, groupByField[0])
	}
	for i := range orderBy {
		orderByFields := view.ViewDataByName(orderBy[i])
		if len(orderByFields) < 1 {
			return fmt.Errorf("ORDER BY column %s not found", orderBy[i])
		}
		view.AddOrderBy(orderByFields[0], direction[i])
	}

	// WHERE
	var cond Conditioner
	if stmt.Where != nil {
		expr := stmt.Where.Expr
		where, err := parseWhere(expr, view, table)
		if err != nil {
			return err
		}
		cond = where
	}

	view.SetGroupBy(groupByFields)
	view.SetLimit(limit)
	view.SetCondition(cond)

	view.AddTable(table)
	s.AddView(view)

	go view.UpdateView()
	return nil
}

func (s *Stream) Query(query string) error {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		err = s.prepareSelect(stmt)
	case *sqlparser.DDL:
		err = s.prepareCreate(stmt)
	}
	return err
}

func (view *View) createFieldMapping(selectedExpr sqlparser.SelectExprs, table Table, deep bool) (fieldName string, params []interface{}, err error) {
	for _, selectedExpr := range selectedExpr {

		switch selectedExpr := selectedExpr.(type) {
		case *sqlparser.StarExpr:
			// return any field as it is considering '*'
			if deep {
				fieldName = "*"
				field := table.Field(table.Fields()[0].name)

				view.AddViewData(&AggregatedViewData{
					SimpleViewData: SimpleViewData{
						field:         field,
						name:          fieldName,
						selectedField: true,
						value:         make(map[string]interface{}),
					},
				})
				return fieldName, params, err
			}

		case *sqlparser.AliasedExpr:

			asFieldName := selectedExpr.As.String()

			switch selectedExpr := selectedExpr.Expr.(type) {
			case *sqlparser.ColName:
				fieldName = selectedExpr.Name.String()
				field := table.Field(fieldName)

				if asFieldName != "" {
					fieldName = asFieldName
				}

				viewData := &AggregatedViewData{
					SimpleViewData: SimpleViewData{
						field:         field,
						name:          fieldName,
						selectedField: true,
						value:         make(map[string]interface{}),
					},
				}
				err = viewData.UpdateModifier("SetAggregatedValue")
				view.AddViewData(viewData)

			case *sqlparser.FuncExpr:
				modfier := selectedExpr.Name.String()
				fieldName, params, err = view.createFieldMapping(selectedExpr.Exprs, table, true)
				viewData := view.ViewData(fieldName)
				if viewData.Name() == "" {
					continue
				}

				viewData.SetParams(params)
				err = viewData.UpdateModifier(strings.ToUpper(modfier))
				if err != nil {
					return fieldName, params, err
				}

				if asFieldName != "" {
					viewData.SetName(asFieldName)
				} else {
					viewData.SetName(fmt.Sprintf("%s(%s)", modfier, fieldName))
				}

			case *sqlparser.SQLVal:
				var paramValue interface{}
				if selectedExpr.Type == sqlparser.StrVal {
					paramValue, err = fromSQLValToString(selectedExpr)
					if err != nil {
						return fieldName, params, err
					}
				} else if selectedExpr.Type == sqlparser.IntVal {
					paramValue, err = fromSQLValToInt(selectedExpr)
					if err != nil {
						return fieldName, params, err
					}
				}
				params = append(params, paramValue)
			}
		}
	}
	return fieldName, params, err
}

func getFieldByStmt(stmt interface{}) (fieldName, extra, method, parameter string, _ error) {
	var columnName string
	column, ok := stmt.(*sqlparser.ColName)
	if !ok {
		funcExpr, ok := stmt.(*sqlparser.FuncExpr)
		if !ok {
			orderExpr, ok := stmt.(*sqlparser.Order)
			if !ok {
				sqlVal, ok := stmt.(*sqlparser.SQLVal)
				if !ok {
					return fieldName, extra, method, parameter, fmt.Errorf("ORDER BY should be collumn name or function")
				}
				fieldName, err := fromSQLValToString(sqlVal)
				if sqlVal.Type == sqlparser.StrVal {
					extra = "strVal"
				} else if sqlVal.Type == sqlparser.IntVal {
					extra = "intVal"
				}

				return fieldName, extra, method, parameter, err
			}
			extra = orderExpr.Direction
			fieldName, _, method, parameter, err := getFieldByStmt(orderExpr.Expr)
			return fieldName, extra, method, parameter, err
		}
		funcName := funcExpr.Name.String()
		aliasedColumn, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			_, ok := funcExpr.Exprs[0].(*sqlparser.StarExpr)
			if !ok {
				return fieldName, extra, method, parameter, fmt.Errorf("ORDER BY should be EXISTENT collumn name or function")
			}
			columnName = "*"
		} else {
			if column, ok := aliasedColumn.Expr.(*sqlparser.ColName); !ok {
				var err error
				sqlVal := aliasedColumn.Expr.(*sqlparser.SQLVal)
				columnName, err = fromSQLValToString(sqlVal)
				parameter = columnName
				method = funcName
				if sqlVal.Type == sqlparser.StrVal {
					extra = "strVal"
				} else if sqlVal.Type == sqlparser.IntVal {
					extra = "intVal"
				}

				if err != nil {
					return fieldName, extra, method, parameter, err
				}
			} else {
				columnName = column.Name.String()
			}
		}
		fieldName = fmt.Sprintf("%s(%s)", funcName, columnName)
	} else {
		fieldName = column.Name.String()
	}
	return fieldName, extra, method, parameter, nil
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

func fromSQLValToString(sqlVal *sqlparser.SQLVal) (str string, err error) {
	str = string(sqlVal.Val)
	return str, err
}

func fromExprToCondition(comparison *sqlparser.ComparisonExpr, c *SimpleCondition, view *View, table Table) (err error) {
	c.operator = Operator(comparison.Operator)
	c.left, err = getFieldOrStatic(comparison.Left, view, table)
	if err != nil {
		return err
	}
	c.right, err = getFieldOrStatic(comparison.Right, view, table)
	return err
}

func parseWhere(expr sqlparser.Expr, view *View, table Table) (c Conditioner, err error) {
	if comparison, ok := expr.(*sqlparser.ComparisonExpr); ok {
		simpleCond := &SimpleCondition{}
		err = fromExprToCondition(comparison, simpleCond, view, table)
		if err != nil {
			return c, err
		}
		c = simpleCond
	} else if or, ok := expr.(*sqlparser.OrExpr); ok {
		orCond := &OrCondition{}
		orCond.right, err = parseWhere(or.Right, view, table)
		if err != nil {
			return c, err
		}
		orCond.left, err = parseWhere(or.Left, view, table)
		if err != nil {
			return c, err
		}
		c = orCond
	} else if and, ok := expr.(*sqlparser.AndExpr); ok {
		andCond := &AndCondition{}
		andCond.right, err = parseWhere(and.Right, view, table)
		if err != nil {
			return c, err
		}
		andCond.left, err = parseWhere(and.Left, view, table)
		if err != nil {
			return c, err
		}
		c = andCond
	} else if paren, ok := expr.(*sqlparser.ParenExpr); ok {
		parenCond := &ParentCondition{}
		parenCond.condition, err = parseWhere(paren.Expr, view, table)
		if err != nil {
			return c, err
		}
		c = parenCond
	}

	return c, err
}

func getFieldOrStatic(expr sqlparser.Expr, view *View, table Table) (ViewData, error) {
	field, extra, method, parameter, _ := getFieldByStmt(expr)
	if extra == "intVal" {
		integer, err := strconv.Atoi(field)
		if err == nil {
			viewData := &SimpleViewData{
				name:    field,
				varType: INTEGER,
				value:   integer,
			}
			err := viewData.UpdateModifier("SetValue")
			return viewData, err
		}
		return nil, err
	} else if extra == "strVal" {
		var value interface{}
		varType := VARCHAR
		value = field
		if strings.HasPrefix(method, "datetime") {
			var err error
			varType = DATETIME
			value, err = parseDate(parameter)
			if err != nil {
				return nil, err
			}
		}
		viewData := &SimpleViewData{
			name:    field,
			varType: varType,
			value:   value,
		}

		err := viewData.UpdateModifier("SetValue")
		return viewData, err
	}
	vds := view.ViewDataByName(field)
	if len(vds) < 1 {
		viewData := &SimpleViewData{
			field: table.Field(field),
			name:  field,
			value: nil,
		}
		err := viewData.UpdateModifier("SetValue")
		view.AddViewData(viewData)

		return viewData, err

	} else {
		return vds[0], nil
	}
}
