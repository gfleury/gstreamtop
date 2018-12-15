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
	if v.field.fieldType != INTEGER {
		return nil, fmt.Errorf("not integer")
	}
	groupByName := getGroupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert SUM.")
		return nil, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		v.data[groupByName] = (v.data[groupByName].(int) + newValue)
	}
	return v.data[groupByName], nil
}

func (v *AggregatedViewData) MAX(newData interface{}, groupByNameArray []string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return nil, fmt.Errorf("not integer")
	}
	groupByName := getGroupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MAX.")
		return nil, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		if newValue > v.data[groupByName].(int) {
			v.data[groupByName] = newValue
		}
	}
	return v.data[groupByName], nil
}

func (v *AggregatedViewData) MIN(newData interface{}, groupByNameArray []string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return 0, fmt.Errorf("not integer")
	}
	groupByName := getGroupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MIN.")
		return 0, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
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
	groupByName := getGroupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if _, ok := newData.(string); !ok {
		fmt.Println("Failed to convert COUNT.")
		return nil, fmt.Errorf("can't read field")
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
	groupByName := getGroupByNameKeyString(groupByNameArray)
	if v.data[groupByName] == nil {
		v.data[groupByName] = average{
			count: 0,
			sum:   0,
		}
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert AVG.")
		return nil, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		calculatedAverage := v.data[groupByName].(average)
		calculatedAverage.count++
		calculatedAverage.sum += newValue
		calculatedAverage.value = calculatedAverage.sum / calculatedAverage.count
		v.data[groupByName] = calculatedAverage
		return calculatedAverage.value, nil
	}
}

func (v *AggregatedViewData) SetAggregatedValue(newData interface{}, groupByName []string) (interface{}, error) {
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert.")
		return nil, fmt.Errorf("not integer")
	} else {
		return setIfGroupByNotEmpty(value, v.data, groupByName)
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
		return nil, err.(error)
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
	groupByName := getGroupByNameKeyString(groupByNameArray)
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
	Value interface{}
}

func (v *AggregatedViewData) KeyArray() []kv {
	rowNumber := v.Length()
	keys := make([]kv, 0, rowNumber)

	for key, value := range v.FetchAll() {
		keys = append(keys, kv{key, value})
	}
	return keys
}

func (v *AggregatedViewData) AggregatedValue() interface{} {
	return v.data[""]
}

func (v *AggregatedViewData) Value() interface{} {
	return v.data["__lastItem"]
}

func getGroupByNameKeyString(groupByName []string) string {
	return strings.Join(groupByName, "/")
}
