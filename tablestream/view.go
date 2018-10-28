package tablestream

import (
	"fmt"
	"sort"

	"github.com/xwb1989/sqlparser"
)

type View struct {
	name          string
	viewData      []*ViewData
	groupByColumn string
	tables        []*Table
}

func CreateView(name, groupBy string) *View {
	view := &View{name: name, groupByColumn: groupBy}
	view.viewData = []*ViewData{}

	return view
}

func (v *View) AddViewData(vd *ViewData) {
	v.viewData = append(v.viewData, vd)
}

func (v *View) GetViewData(name string) *ViewData {
	for i, viewData := range v.viewData {
		if viewData.name == name {
			return v.viewData[i]
		}
	}
	return &ViewData{}
}

func (v *View) AddTable(t *Table) {
	v.tables = append(v.tables, t)
	t.typeInstance[v.name] = make(chan map[string]string)
}

func (v *View) GetViewDataByFieldName(name string) []*ViewData {
	viewDatas := []*ViewData{}
	for i, viewData := range v.viewData {
		if viewData.field.name == name {
			viewDatas = append(viewDatas, v.viewData[i])
		}
	}
	return viewDatas
}

func (v *View) UpdateView() {
	for {
		for _, table := range v.tables {
			select {
			case newData := <-table.typeInstance[v.name]:
				//fmt.Println(newData)
				for key, value := range newData {
					for _, viewData := range v.GetViewDataByFieldName(key) {
						err := viewData.CallUpdateValue(value, newData[v.groupByColumn])
						if err != nil {
							fmt.Printf("failed to update value on %s:%s\n", v.name, viewData.name)
						}
					}
				}
			}
		}
	}
}

func (v *View) GetIntViewData(idx int) []int {
	vd := v.viewData[idx]
	if vd.field.fieldType != INTEGER {
		return []int{}
	}

	keys := make([]string, 0, len(vd.data))
	for key := range vd.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	ret := make([]int, len(vd.data))

	for j, key := range keys {
		ret[j] = vd.data[key].(int)
		j++
	}
	return ret
}

func (v *View) GetStringViewData(idx int) []string {
	vd := v.viewData[idx]
	if vd.field.fieldType != VARCHAR {
		return []string{}
	}

	keys := make([]string, 0, len(vd.data))
	for key := range vd.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	ret := make([]string, len(vd.data))

	for j, key := range keys {
		ret[j] = vd.data[key].(string)
		j++
	}
	return ret
}

func (view *View) createFieldMapping(selectedExpr sqlparser.SelectExprs, table *Table, deep bool) string {

	for _, selectedExpr := range selectedExpr {

		switch selectedExpr := selectedExpr.(type) {
		case *sqlparser.StarExpr:
			// return any field as it is considering '*'
			if deep {
				fieldName := "*"
				field := table.GetField(table.fields[0].name)

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
				field := table.GetField(fieldName)

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
				viewData := view.GetViewData(fieldName)
				if viewData.name == "" {
					continue
				}
				viewData.UpdateModifier(modfier)
			}
		}
	}
	return ""
}
