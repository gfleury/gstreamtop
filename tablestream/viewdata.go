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

func (v *ViewData) UpdateModifier(mod string) {
	switch mod {
	case "COUNT":
		v.varType = INTEGER
	default:
		v.varType = v.field.fieldType
	}
	t := reflect.ValueOf(v)
	m := t.MethodByName(mod)
	v.updateValue = m
	v.modifier = mod
}

func (v *ViewData) SUM(newData interface{}, groupByName string) error {
	if v.field.fieldType != INTEGER {
		return fmt.Errorf("not integer")
	}
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert SUM.")
		return fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		v.data[groupByName] = (v.data[groupByName].(int) + newValue)
	}
	return nil
}

func (v *ViewData) MAX(newData interface{}, groupByName string) error {
	if v.field.fieldType != INTEGER {
		return fmt.Errorf("not integer")
	}
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MAX.")
		return fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		if newValue > v.data[groupByName].(int) {
			v.data[groupByName] = newValue
		}
	}
	return nil
}

func (v *ViewData) MIN(newData interface{}, groupByName string) error {
	if v.field.fieldType != INTEGER {
		return fmt.Errorf("not integer")
	}
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert MIN.")
		return fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		if newValue < v.data[groupByName].(int) {
			v.data[groupByName] = newValue
		}
	}
	return nil
}

func (v *ViewData) COUNT(newData interface{}, groupByName string) error {
	//if v.field.fieldType != INTEGER {
	//	return fmt.Errorf("not integer")
	//}
	if v.data[groupByName] == nil {
		v.data[groupByName] = 0
	}
	if _, ok := newData.(string); !ok {
		fmt.Println("Failed to convert COUNT.")
		return fmt.Errorf("can't read field")
	}

	v.data[groupByName] = (v.data[groupByName].(int) + 1)

	return nil
}

type average struct {
	AnalyticFunc
	count, sum, value int
}

func (a average) Value() int {
	return a.value
}

func (v *ViewData) AVG(newData interface{}, groupByName string) error {
	if v.data[groupByName] == nil {
		v.data[groupByName] = average{
			count: 0,
			sum:   0,
		}
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert AVG.")
		return fmt.Errorf("can't read field")
	} else {
		newValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		calculatedAverage := v.data[groupByName].(average)
		calculatedAverage.count++
		calculatedAverage.sum += newValue
		calculatedAverage.value = calculatedAverage.sum / calculatedAverage.count
		v.data[groupByName] = calculatedAverage
	}

	return nil
}

func (v *ViewData) SetValue(newData interface{}, groupByName string) error {
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert.")
		return fmt.Errorf("not integer")
	} else {
		v.data[groupByName] = value
	}
	return nil
}

func (v *ViewData) fetch() map[string]interface{} {
	for v.data == nil {
		time.Sleep(100 * time.Millisecond)
	}
	return v.data
}

func (v *ViewData) CallUpdateValue(value interface{}, groupByName string) error {
	result := v.updateValue.Call([]reflect.Value{reflect.ValueOf(value), reflect.ValueOf(groupByName)})
	err := result[0].Interface()
	if err != nil {
		return err.(error)
	}
	return nil
}

func (v *ViewData) URLIFY(newData interface{}, groupByName string) error {
	if v.field.fieldType != VARCHAR {
		return fmt.Errorf("not varchar")
	}
	if v.data[groupByName] == nil {
		v.data[groupByName] = ""
	}
	if value, ok := newData.(string); !ok {
		fmt.Println("Failed to convert URLIFY.")
		return fmt.Errorf("can't read field")
	} else {
		v.data[groupByName] = strings.Split(value, "?")[0]
	}
	return nil
}
