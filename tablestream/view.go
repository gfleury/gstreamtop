package tablestream

import (
	"fmt"
	"os"
	"sort"
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

func (v *View) ViewData(name string) *ViewData {
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

func (v *View) ViewDataByFieldName(name string) []*ViewData {
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
					for _, viewData := range v.ViewDataByFieldName(key) {
						err := viewData.CallUpdateValue(value, newData[v.groupByColumn])
						if err != nil {
							fmt.Printf("failed to update value on %s:%s %s %s\n", v.name, viewData.name, value, err.Error())
						}
					}
				}
			}
		}
	}
}

func (v *View) IntViewData(idx int) []int {
	vd := v.viewData[idx]
	// if vd.field.fieldType != INTEGER {
	// 	return []int{}
	// }

	keys := make([]string, 0, len(vd.data))
	for key := range vd.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	ret := make([]int, len(vd.data))

	for j, key := range keys {
		var ok bool
		ret[j], ok = vd.data[key].(int)
		if !ok {
			ret[j] = vd.data[key].(AnalyticFunc).Value()
		}
		j++
	}
	return ret
}

func (v *View) StringViewData(idx int) []string {
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

func (v *View) FetchAllRows() [][]string {
	allRows := make([][]string, len(v.viewData[0].data)+1)

	for i, column := range v.viewData {
		columnName := column.name

		allRows[0] = append(allRows[0], columnName)

		dataType := column.field.fieldType
		if column.modifier != "SetValue" {
			dataType = INTEGER
		}

		switch dataType {
		case VARCHAR:
			data := v.StringViewData(i)
			for j := range data {
				allRows[j+1] = append(allRows[j+1], data[j])
			}
		case INTEGER:
			data := v.IntViewData(i)
			for i := range data {
				allRows[i+1] = append(allRows[i+1], fmt.Sprintf("%d", data[i]))
			}
		}
	}
	return allRows
}

func (v *View) PrintView() {
	TableWrite(v, os.Stdout)
}
