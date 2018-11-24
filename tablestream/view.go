package tablestream

import (
	"fmt"
	"os"
	"sort"
)

type View struct {
	name         string
	viewData     []*ViewData
	groupByField *ViewData
	orderBy      struct {
		orderByField *ViewData
		direction    string
	}
	tables []*Table
	limit  int
}

func CreateView(name string) *View {
	view := &View{name: name}
	view.viewData = []*ViewData{}
	view.limit = 0

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

func (v *View) ViewDataByName(name string) []*ViewData {
	viewDatas := []*ViewData{}
	for i, viewData := range v.viewData {
		if viewData.name == name {
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
				groupBy, _ := v.groupByField.CallUpdateValue(newData[v.groupByField.field.name], "")
				for key, value := range newData {
					for _, viewData := range v.ViewDataByFieldName(key) {
						_, err := viewData.CallUpdateValue(value, groupBy.(string)) //newData[v.groupByColumn])
						if err != nil {
							viewData.field.AddError(fmt.Errorf("failed to update value on %s:%s %s %s\n", v.name, viewData.name, value, err.Error()))
						}
					}
				}
			}
		}
	}
}

func (v *View) IntViewData(idx int, keys []string) []int {
	vd := v.viewData[idx]
	// if vd.field.fieldType != INTEGER {
	// 	return []int{}
	// }

	// keys := make([]string, 0, len(vd.data))
	// for key := range vd.data {
	// 	keys = append(keys, key)
	// }
	// sort.Strings(keys)

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

func (v *View) StringViewData(idx int, keys []string) []string {
	vd := v.viewData[idx]
	if vd.field.fieldType != VARCHAR {
		return []string{}
	}

	// keys := make([]string, 0, len(vd.data))
	// for key := range vd.data {
	// 	keys = append(keys, key)
	// }
	// sort.Strings(keys)

	ret := make([]string, len(vd.data))

	for j, key := range keys {
		ret[j] = vd.data[key].(string)
		j++
	}
	return ret
}

func (v *View) FetchAllRows() [][]string {
	allRows := make([][]string, len(v.viewData[0].data)+1)

	orderedKeys := v.OrderedKeys()

	for i, column := range v.viewData {
		columnName := column.name

		allRows[0] = append(allRows[0], columnName)

		dataType := column.varType

		switch dataType {
		case VARCHAR:
			data := v.StringViewData(i, orderedKeys)
			for j := range data {
				allRows[j+1] = append(allRows[j+1], data[j])
			}
		case INTEGER:
			data := v.IntViewData(i, orderedKeys)
			for i := range data {
				allRows[i+1] = append(allRows[i+1], fmt.Sprintf("%d", data[i]))
			}
		}
	}

	if v.limit == 0 {
		return allRows
	}

	return allRows[:v.limit+1]
}

func (v *View) PrintView() {
	TableWrite(v, os.Stdout)
}

func (v *View) SetLimit(limit int) {
	v.limit = limit
}

func (v *View) SetOrderBy(orderByFields []*ViewData, direction string) {
	// TODO Handle more than one GROUP BY
	v.orderBy.orderByField = orderByFields[0]
	v.orderBy.direction = direction
}

func (v *View) SetGroupBy(groupByFields []*ViewData) {
	// TODO Handle more than one GROUP BY
	groupByField := groupByFields[0]
	v.groupByField = &ViewData{
		data:        make(map[string]interface{}),
		field:       groupByField.field,
		modifier:    groupByField.modifier,
		name:        groupByField.name,
		updateValue: groupByField.updateValue,
		varType:     groupByField.varType,
	}

}

func (v *View) OrderedKeys() []string {
	type kv struct {
		Key   string
		Value interface{}
	}

	vd := v.viewData[0]

	for _, vd = range v.viewData {
		if vd == v.orderBy.orderByField {
			break
		}
	}

	keys := make([]kv, 0, len(vd.data))
	for key := range vd.data {
		keys = append(keys, kv{key, vd.data[key]})
	}

	ascOrder := true
	if v.orderBy.direction != "asc" {
		ascOrder = false
	}

	if v.orderBy.orderByField != nil {
		sort.Slice(keys, func(i, j int) bool {
			if vd.varType == INTEGER {
				if ascOrder {
					return keys[i].Value.(int) > keys[j].Value.(int)
				} else {
					return keys[i].Value.(int) < keys[j].Value.(int)
				}
			} else if vd.varType == VARCHAR {
				if ascOrder {
					return keys[i].Value.(string) > keys[j].Value.(string)
				} else {
					return keys[i].Value.(string) < keys[j].Value.(string)
				}
			}
			return false
		})
	}

	orderedKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		orderedKeys = append(orderedKeys, key.Key)
	}

	if v.orderBy.orderByField == nil {
		sort.Strings(orderedKeys)
	}

	return orderedKeys
}
