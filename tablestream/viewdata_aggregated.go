package tablestream

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type AggregatedViewData struct {
	SimpleViewData
}

const lastItemKey = "__lastItem"

type AggregatedValue struct {
	value   interface{}
	groupBy []string
}

type AnalyticFunc interface {
	Value() int
}

func (v *AggregatedViewData) UpdateModifier(mod string) error {
	switch mod {
	case "COUNT":
		v.varType = INTEGER
	default:
		v.varType = v.field.fieldType
	}
	t := reflect.ValueOf(v)
	m := t.MethodByName(mod)
	if !m.IsValid() {
		return fmt.Errorf("function %s not found", mod)
	}
	v.updateValue = m
	return nil
}

func (v *AggregatedViewData) SUM(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	vdata := v.castValue()
	if vdata[groupByName] == nil {
		vdata[groupByName] = 0
	}
	if v.field.fieldType != INTEGER {
		return vdata[groupByName], fmt.Errorf("not integer")
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert SUM.")
		return vdata[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return vdata[groupByName], err
		}
		vdata[groupByName] = (vdata[groupByName].(int) + newValue)
	}
	return vdata[groupByName], nil
}

func (v *AggregatedViewData) MAX(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	vdata := v.castValue()
	if vdata[groupByName] == nil {
		vdata[groupByName] = 0
	}
	if v.field.fieldType != INTEGER {
		return vdata[groupByName], fmt.Errorf("not integer")
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MAX.")
		return vdata[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return vdata[groupByName], err
		}
		if newValue > vdata[groupByName].(int) {
			vdata[groupByName] = newValue
		}
	}
	return vdata[groupByName], nil
}

func (v *AggregatedViewData) MIN(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	vdata := v.castValue()
	if vdata[groupByName] == nil {
		vdata[groupByName] = 0
	}
	if v.field.fieldType != INTEGER {
		return vdata[groupByName], fmt.Errorf("not integer")
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MIN.")
		return vdata[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return vdata[groupByName], err
		}
		if newValue < vdata[groupByName].(int) {
			vdata[groupByName] = newValue
		}
	}
	return vdata[groupByName], nil
}

func (v *AggregatedViewData) COUNT(newData interface{}, groupByNameArray []string) (interface{}, error) {
	//if v.field.fieldType != INTEGER {
	//	return fmt.Errorf("not integer")
	//}
	groupByName := groupByNameKeyString(groupByNameArray)
	vdata := v.castValue()
	if vdata[groupByName] == nil {
		vdata[groupByName] = 0
	}
	if _, ok := newData.(string); !ok {
		fmt.Println("Failed to convert COUNT.")
		return vdata[groupByName], fmt.Errorf("can't read field")
	}

	vdata[groupByName] = (vdata[groupByName].(int) + 1)

	return vdata[groupByName], nil
}

type average struct {
	AnalyticFunc
	count, sum, value int
}

func (a average) Value() int {
	return a.value
}

func (v *AggregatedViewData) AVG(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	vdata := v.castValue()
	if vdata[groupByName] == nil {
		vdata[groupByName] = average{
			count: 0,
			sum:   0,
		}
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert AVG.")
		return vdata[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return vdata[groupByName], err
		}
		calculatedAverage := vdata[groupByName].(average)
		calculatedAverage.count++
		calculatedAverage.sum += newValue
		calculatedAverage.value = calculatedAverage.sum / calculatedAverage.count
		vdata[groupByName] = calculatedAverage
		return calculatedAverage.value, nil
	}
}

func (v *AggregatedViewData) SetAggregatedValue(newData interface{}, groupByNameArray []string) (interface{}, error) {
	vdata := v.castValue()
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert.")
		return nil, fmt.Errorf("not integer")
	} else {
		switch v.VarType() {
		case INTEGER:
			intValue, err := strconv.Atoi(value)
			if err != nil {
				retValue, _ := setIfGroupByNotEmpty(0, vdata, groupByNameArray)
				return retValue, err
			}
			return setIfGroupByNotEmpty(intValue, vdata, groupByNameArray)
		case DATETIME:
			dtValue, err := parseDate(value)
			if err != nil {
				retValue, _ := setIfGroupByNotEmpty(time.Now(), vdata, groupByNameArray)
				return retValue, err
			}
			return setIfGroupByNotEmpty(dtValue, vdata, groupByNameArray)
		}
		return setIfGroupByNotEmpty(value, vdata, groupByNameArray)
	}

}

func (v *AggregatedViewData) FetchAll() map[string]interface{} {
	vdata := v.castValue()
	for vdata == nil {
		time.Sleep(100 * time.Millisecond)
	}
	return vdata
}

func (v *AggregatedViewData) CallUpdateValue(value interface{}) (interface{}, error) {
	aggregatedValue := value.(AggregatedValue)

	result := v.updateValue.Call([]reflect.Value{reflect.ValueOf(aggregatedValue.value), reflect.ValueOf(aggregatedValue.groupBy)})
	err := result[1].Interface()
	if err != nil {
		return result[0].Interface(), err.(error)
	}
	return result[0].Interface(), nil
}

func (v *AggregatedViewData) URLIFY(newData interface{}, groupByName []string) (interface{}, error) {
	vdata := v.castValue()
	if v.field.fieldType != VARCHAR {
		return "", fmt.Errorf("not varchar")
	}
	// if vdata[groupByName] == nil {
	// 	vdata[groupByName] = ""
	// }
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert URLIFY.")
		return "", fmt.Errorf("can't read field")
	} else {
		return setIfGroupByNotEmpty(strings.Split(value, "?")[0], vdata, groupByName)
	}
}

func setIfGroupByNotEmpty(value interface{}, data map[string]interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	if len(groupByName) == 0 {
		return value, nil
	}
	data[groupByName] = value

	return data[groupByName], nil
}

func (v *AggregatedViewData) Length() int {
	vdata := v.castValue()
	return len(vdata)
}

func (v *AggregatedViewData) Fetch(key string) interface{} {
	vdata := v.castValue()
	return vdata[key]
}

type kv struct {
	Key   string
	Value []interface{}
}

func mergeKv(keys *[]kv, newKeys *[]kv, idx int) {
	for _, newKey := range *newKeys {
		added := false
		for j, key := range *keys {
			if key.Key == newKey.Key {
				(*keys)[j].Value = append((*keys)[j].Value, newKey.Value...)
				//(*keys)[j].Value[idx] = newKey.Value[0]
				added = true
			}
		}
		if !added {
			*keys = append(*keys, newKey)
		}
	}
}

func (v *AggregatedViewData) KeyArray() []kv {
	rowNumber := v.Length()
	keys := make([]kv, 0, rowNumber)

	for key, value := range v.FetchAll() {
		keys = append(keys, kv{key, []interface{}{value}})
	}
	return keys
}

func (v *AggregatedViewData) Value() interface{} {
	vdata := v.castValue()
	return vdata[lastItemKey]
}

func groupByNameKeyString(groupByName []string) string {
	return strings.Join(groupByName, "/")
}

func (v *AggregatedViewData) castValue() map[string]interface{} {
	vdata := v.value.(map[string]interface{})
	return vdata
}

func (v *AggregatedViewData) TUMBLING(newData interface{}, groupByNameArray []string) (interface{}, error) {
	vdata := v.castValue()
	dtValue, err := parseDate(newData.(string))
	if err != nil {
		retValue, _ := setIfGroupByNotEmpty(time.Now(), vdata, groupByNameArray)
		return retValue, err
	}
	return setIfGroupByNotEmpty(dtValue, vdata, groupByNameArray)
}

func (v *AggregatedViewData) SESSION(newData interface{}, groupByNameArray []string) (interface{}, error) {
	vdata := v.castValue()
	dtValue, err := parseDate(newData.(string))
	if err != nil {
		retValue, _ := setIfGroupByNotEmpty(time.Now(), vdata, groupByNameArray)
		return retValue, err
	}
	return setIfGroupByNotEmpty(dtValue, vdata, groupByNameArray)
}
