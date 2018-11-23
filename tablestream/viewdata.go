package tablestream

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type ViewData struct {
	name        string
	data        map[string]interface{}
	field       *Field
	table       *Table
	updateValue reflect.Value
	modifier    string
	varType     fieldType
}

type AnalyticFunc interface {
	Value() int
}

func (v *ViewData) UpdateModifier(mod string) error {
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

func (v *ViewData) SUM(newData interface{}, groupByName string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return nil, fmt.Errorf("not integer")
	}
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

func (v *ViewData) MAX(newData interface{}, groupByName string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return nil, fmt.Errorf("not integer")
	}
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

func (v *ViewData) MIN(newData interface{}, groupByName string) (interface{}, error) {
	if v.field.fieldType != INTEGER {
		return 0, fmt.Errorf("not integer")
	}
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

func (v *ViewData) COUNT(newData interface{}, groupByName string) (interface{}, error) {
	//if v.field.fieldType != INTEGER {
	//	return fmt.Errorf("not integer")
	//}
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

func (v *ViewData) AVG(newData interface{}, groupByName string) (interface{}, error) {
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

func (v *ViewData) SetValue(newData interface{}, groupByName string) (interface{}, error) {
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert.")
		return nil, fmt.Errorf("not integer")
	} else {
		return setIfGroupByNotEmpty(value, v.data, groupByName)
	}
}

func (v *ViewData) fetch() map[string]interface{} {
	for v.data == nil {
		time.Sleep(100 * time.Millisecond)
	}
	return v.data
}

func (v *ViewData) CallUpdateValue(value interface{}, groupByName string) (interface{}, error) {
	result := v.updateValue.Call([]reflect.Value{reflect.ValueOf(value), reflect.ValueOf(groupByName)})
	err := result[1].Interface()
	if err != nil {
		return nil, err.(error)
	}
	return result[0].Interface(), nil
}

func (v *ViewData) URLIFY(newData interface{}, groupByName string) (interface{}, error) {
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

func setIfGroupByNotEmpty(value interface{}, data map[string]interface{}, groupByName string) (interface{}, error) {
	if len(groupByName) == 0 {
		return value, nil
	}
	data[groupByName] = value

	return data[groupByName], nil
}
