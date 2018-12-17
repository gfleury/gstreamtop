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
	data map[string]interface{}
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
	v.modifier = mod
	return nil
}

func (v *AggregatedViewData) SUM(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if v.field.fieldType != INTEGER {
		return v.data[groupByName], fmt.Errorf("not integer")
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert SUM.")
		return v.data[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return v.data[groupByName], err
		}
		v.data[groupByName] = (v.data[groupByName].(int) + newValue)
	}
	return v.data[groupByName], nil
}

func (v *AggregatedViewData) MAX(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if v.field.fieldType != INTEGER {
		return v.data[groupByName], fmt.Errorf("not integer")
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MAX.")
		return v.data[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return v.data[groupByName], err
		}
		if newValue > v.data[groupByName].(int) {
			v.data[groupByName] = newValue
		}
	}
	return v.data[groupByName], nil
}

func (v *AggregatedViewData) MIN(newData interface{}, groupByNameArray []string) (interface{}, error) {
	groupByName := groupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if v.field.fieldType != INTEGER {
		return v.data[groupByName], fmt.Errorf("not integer")
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MIN.")
		return v.data[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return v.data[groupByName], err
		}
		if newValue < v.data[groupByName].(int) {
			v.data[groupByName] = newValue
		}
	}
	return v.data[groupByName], nil
}

func (v *AggregatedViewData) COUNT(newData interface{}, groupByNameArray []string) (interface{}, error) {
	//if v.field.fieldType != INTEGER {
	//	return fmt.Errorf("not integer")
	//}
	groupByName := groupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if _, ok := newData.(string); !ok {
		fmt.Println("Failed to convert COUNT.")
		return v.data[groupByName], fmt.Errorf("can't read field")
	}

	v.data[groupByName] = (v.data[groupByName].(int) + 1)

	return v.data[groupByName], nil
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
	if v.data[groupByName] == nil {
		v.data[groupByName] = average{
			count: 0,
			sum:   0,
		}
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert AVG.")
		return v.data[groupByName], fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return v.data[groupByName], err
		}
		calculatedAverage := v.data[groupByName].(average)
		calculatedAverage.count++
		calculatedAverage.sum += newValue
		calculatedAverage.value = calculatedAverage.sum / calculatedAverage.count
		v.data[groupByName] = calculatedAverage
		return calculatedAverage.value, nil
	}
}

func (v *AggregatedViewData) SetAggregatedValue(newData interface{}, groupByNameArray []string) (interface{}, error) {
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert.")
		return nil, fmt.Errorf("not integer")
	} else {
		if v.VarType() == INTEGER {
			intValue, err := strconv.Atoi(value)
			if err != nil {
				retValue, _ := setIfGroupByNotEmpty(0, v.data, groupByNameArray)
				return retValue, err
			}
			return setIfGroupByNotEmpty(intValue, v.data, groupByNameArray)
		}
		return setIfGroupByNotEmpty(value, v.data, groupByNameArray)
	}

}

func (v *AggregatedViewData) FetchAll() map[string]interface{} {
	for v.data == nil {
		time.Sleep(100 * time.Millisecond)
	}
	return v.data
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
	if v.field.fieldType != VARCHAR {
		return "", fmt.Errorf("not varchar")
	}
	// if v.data[groupByName] == nil {
	// 	v.data[groupByName] = ""
	// }
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert URLIFY.")
		return "", fmt.Errorf("can't read field")
	} else {
		return setIfGroupByNotEmpty(strings.Split(value, "?")[0], v.data, groupByName)
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
	return len(v.data)
}

func (v *AggregatedViewData) Fetch(key string) interface{} {
	return v.data[key]
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
	return v.data[lastItemKey]
}

func groupByNameKeyString(groupByName []string) string {
	return strings.Join(groupByName, "/")
}
