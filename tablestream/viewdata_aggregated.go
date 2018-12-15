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

func (v *AggregatedViewData) SUM(newData interface{}, groupByName []string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return nil, fmt.Errorf("not integer")
	}
	if getKeyDeep(groupByName, v.data) == nil {
		setKeyDeep(0, groupByName, v.data)
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert SUM.")
		return nil, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		// v.data[groupByName] = (v.data[groupByName].(int) + newValue)
		setKeyDeep((getKeyDeep(groupByName, v.data).(int) + newValue), groupByName, v.data)
	}
	return getKeyDeep(groupByName, v.data), nil
}

func (v *AggregatedViewData) MAX(newData interface{}, groupByName []string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return nil, fmt.Errorf("not integer")
	}
	if getKeyDeep(groupByName, v.data) == nil {
		setKeyDeep(0, groupByName, v.data)
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MAX.")
		return nil, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		if newValue > getKeyDeep(groupByName, v.data).(int) {
			// v.data[groupByName] = newValue
			setKeyDeep(newValue, groupByName, v.data)
		}
	}
	return getKeyDeep(groupByName, v.data), nil
}

func (v *AggregatedViewData) MIN(newData interface{}, groupByName []string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return 0, fmt.Errorf("not integer")
	}
	if getKeyDeep(groupByName, v.data) == nil {
		setKeyDeep(0, groupByName, v.data)
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MIN.")
		return 0, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		if newValue < getKeyDeep(groupByName, v.data).(int) {
			// v.data[groupByName] = newValue
			setKeyDeep(newValue, groupByName, v.data)
		}
	}
	return getKeyDeep(groupByName, v.data), nil
}

func (v *AggregatedViewData) COUNT(newData interface{}, groupByName []string) (interface{}, error) {
	//if v.field.fieldType != INTEGER {
	//	return fmt.Errorf("not integer")
	//}
	if getKeyDeep(groupByName, v.data) == nil {
		setKeyDeep(0, groupByName, v.data)
	}
	if _, ok := newData.(string); !ok {
		fmt.Println("Failed to convert COUNT.")
		return nil, fmt.Errorf("can't read field")
	}

	setKeyDeep((getKeyDeep(groupByName, v.data).(int) + 1), groupByName, v.data)

	return getKeyDeep(groupByName, v.data), nil
}

type average struct {
	AnalyticFunc
	count, sum, value int
}

func (a average) Value() int {
	return a.value
}

func (v *AggregatedViewData) AVG(newData interface{}, groupByName []string) (interface{}, error) {
	if getKeyDeep(groupByName, v.data) == nil {
		setKeyDeep(average{
			count: 0,
			sum:   0,
		}, groupByName, v.data)
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert AVG.")
		return nil, fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		calculatedAverage := getKeyDeep(groupByName, v.data).(average)
		calculatedAverage.count++
		calculatedAverage.sum += newValue
		calculatedAverage.value = calculatedAverage.sum / calculatedAverage.count
		setKeyDeep(calculatedAverage, groupByName, v.data)
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

func setIfGroupByNotEmpty(value interface{}, data map[string]interface{}, groupByName []string) (interface{}, error) {
	if len(groupByName) == 0 {
		return value, nil
	}
	// data[groupByName] = value
	setKeyDeep(value, groupByName, data)

	return getKeyDeep(groupByName, data), nil
}

func (v *AggregatedViewData) Length() int {
	return len(v.data)
}

func (v *AggregatedViewData) Fetch(key []string) interface{} {
	return getKeyDeep(key, v.data)
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

func getKeyDeep(key []string, mape map[string]interface{}) interface{} {
	if item, ok := mape[key[0]].(map[string]interface{}); ok {
		return getKeyDeep(key[1:], item)
	}
	return mape[key[0]]

}

func setKeyDeep(value interface{}, key []string, mape map[string]interface{}) interface{} {
	if item, ok := mape[key[0]].(map[string]interface{}); ok {
		return setKeyDeep(value, key[1:], item)
	}
	mape[key[0]] = value
	return value
}
