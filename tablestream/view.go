package tablestream

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type OrderBy struct {
	orderByField ViewData
	direction    string
}
type View struct {
	name          string
	viewData      []ViewData
	groupByFields []ViewData
	orderBy       []OrderBy
	condition     Conditioner
	tables        []Table
	limit         int
	lock          sync.Mutex
	errors        []error
}

func CreateView(name string) *View {
	view := &View{name: name}
	view.viewData = []ViewData{}
	view.limit = 0

	return view
}

func (v *View) AddViewData(vd ViewData) {
	v.viewData = append(v.viewData, vd)
}

func (v *View) ViewData(name string) ViewData {
	for i, viewData := range v.viewData {
		if viewData.Name() == name {
			return v.viewData[i]
		}
	}
	return &AggregatedViewData{}
}

func (v *View) AddTable(t Table) {
	v.tables = append(v.tables, t)
	t.Lock()
	t.SetTypeInstance(v.name, make(chan map[string]string))
	t.Unlock()
}

func (v *View) ViewDataByFieldName(name string) []ViewData {
	viewDatas := []ViewData{}
	for i, viewData := range v.viewData {
		if viewData.Field().name == name {
			viewDatas = append(viewDatas, v.viewData[i])
		}
	}
	return viewDatas
}

func (v *View) ViewDataByName(name string) []ViewData {
	viewDatas := []ViewData{}
	for i, viewData := range v.viewData {
		if viewData.Name() == name {
			viewDatas = append(viewDatas, v.viewData[i])
		}
	}
	return viewDatas
}

func (v *View) UpdateView() {
	for {
		for _, table := range v.tables {
			table.Lock()
			tableChan := table.TypeInstance(v.name)
			table.Unlock()
			select {
			case newData := <-tableChan:
				if !v.evaluateWhere(newData) {
					continue
				}
				v.lock.Lock()
				groupBy := make([]string, len(v.groupByFields))
				for idx, groupByField := range v.groupByFields {
					var ok bool
					groupByIfc, err := groupByField.CallUpdateValue(AggregatedValue{value: newData[groupByField.Field().name], groupBy: []string{""}})
					if err != nil {
						v.AddError(fmt.Errorf("failed to update value on %s:%s %s", v.name, groupByField.Field().name, err.Error()))
						continue
					}
					if groupBy[idx], ok = groupByIfc.(string); !ok {
						groupBy[idx] = fmt.Sprintf("%d", groupByIfc.(int))
					}
				}
				for key, value := range newData {
					for _, viewData := range v.ViewDataByFieldName(key) {
						_, err := viewData.CallUpdateValue(AggregatedValue{value: value, groupBy: groupBy})
						if err != nil {
							v.AddError(fmt.Errorf("failed to update value on %s:%s %s %s", v.name, viewData.Name(), value, err.Error()))
						}
					}
				}
				v.lock.Unlock()
			}
		}
	}
}

func (v *View) IntViewData(idx int, keys []string) []int {
	vd := v.viewData[idx]

	ret := make([]int, len(keys))

	for j, key := range keys {
		var ok bool
		kvalue := vd.Fetch(key)
		ret[j], ok = kvalue.(int)
		if !ok {
			if analyticFunc, ok := kvalue.(AnalyticFunc); !ok {
				var err error
				ret[j], err = strconv.Atoi(kvalue.(string))
				if err != nil {
					ret[j] = 0
				}
			} else {
				ret[j] = analyticFunc.Value()
			}
		}
		j++
	}
	return ret
}

func (v *View) StringViewData(idx int, keys []string) []string {
	vd := v.viewData[idx]
	if vd.Field().fieldType != VARCHAR {
		return []string{}
	}

	ret := make([]string, len(keys))

	for j, key := range keys {
		ret[j] = vd.Fetch(key).(string)
		j++
	}
	return ret
}

func (v *View) DatetimeViewData(idx int, keys []string) []time.Time {
	vd := v.viewData[idx]
	if vd.Field().fieldType != DATETIME {
		return []time.Time{}
	}

	ret := make([]time.Time, len(keys))

	for j, key := range keys {
		ret[j] = vd.Fetch(key).(time.Time)
		j++
	}
	return ret
}

func (v *View) FetchAllRows() [][]string {
	v.lock.Lock()
	defer v.lock.Unlock()

	rowNumber := v.viewData[0].Length()

	allRows := make([][]string, rowNumber+1)

	orderedKeys := v.OrderedKeys()

	for i, column := range v.viewData {
		if !column.SelectedField() {
			continue
		}
		columnName := column.Name()

		allRows[0] = append(allRows[0], columnName)

		dataType := column.VarType()

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
		case DATETIME:
			data := v.DatetimeViewData(i, orderedKeys)
			for k := range data {
				allRows[k+1] = append(allRows[k+1], data[k].Format(time.ANSIC))
			}
		}
	}

	if v.limit == 0 || rowNumber == 0 || rowNumber <= v.limit {
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

func (v *View) AddOrderBy(orderByField ViewData, direction string) {
	v.orderBy = append(v.orderBy, OrderBy{orderByField, direction})
}

func (v *View) SetGroupBy(groupByFields []ViewData) {
	v.groupByFields = groupByFields
}

func (v *View) OrderedKeys() []string {

	vd := make([]ViewData, len(v.orderBy))
	var ascOrder []bool

	for _, vdItem := range v.viewData {
		for i := range v.orderBy {
			if vdItem == v.orderBy[i].orderByField {
				vd[i] = vdItem
				ascOrder = append(ascOrder, v.orderBy[i].direction == "asc")
			}
		}
	}

	if len(vd) == 0 {
		vd = v.viewData[0:1]
	}

	var keys []kv
	for idx := range vd {
		newKeys := vd[idx].KeyArray()
		mergeKv(&keys, &newKeys, idx)
	}

	if len(v.orderBy) > 0 {
		sort.Slice(keys, func(i, j int) (status bool) {
			for idx := range vd {
				if len(keys[i].Value) <= idx || len(keys[j].Value) <= idx {
					continue
				}
				if vd[idx].VarType() == INTEGER {
					if keys[i].Value[idx].(int) == keys[j].Value[idx].(int) {
						if idx == 0 {
							status = (keys[i].Key > keys[j].Key)
							continue
						} else {
							status = (keys[i].Key > keys[j].Key) || status
							continue
						}
					}
					if ascOrder[idx] {
						return keys[i].Value[idx].(int) > keys[j].Value[idx].(int)
					} else {
						return keys[i].Value[idx].(int) < keys[j].Value[idx].(int)
					}
				} else if vd[idx].VarType() == VARCHAR {
					if keys[i].Value[idx].(string) == keys[j].Value[idx].(string) {
						if idx == 0 {
							status = keys[i].Key > keys[j].Key
							continue
						} else {
							status = keys[i].Key > keys[j].Key || status
							continue
						}
					}
					if ascOrder[idx] {
						return keys[i].Value[idx].(string) > keys[j].Value[idx].(string)
					} else {
						return keys[i].Value[idx].(string) < keys[j].Value[idx].(string)

					}
				}
			}
			return status
		})
	}

	orderedKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		orderedKeys = append(orderedKeys, key.Key)
	}

	if len(v.orderBy) == 0 {
		sort.Strings(orderedKeys)
	}

	return orderedKeys
}

func (v *View) AddError(err error) {
	maxSize := 1024
	v.errors = append(v.errors, err)
	if len(v.errors) > maxSize {
		v.errors[maxSize] = nil
		v.errors = v.errors[:maxSize]
	}
}

func (v *View) SetCondition(cond Conditioner) {
	v.condition = cond
}

func (v *View) evaluateWhere(row map[string]string) bool {
	if v.condition != nil {
		return v.condition.Evaluate(row)
	}
	return true
}
