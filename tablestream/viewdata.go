package tablestream

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type ViewData struct {
	name        string
	data        map[string]interface{}
	field       *Field
	table       *Table
	updateValue reflect.Value
}

func (v *ViewData) UpdateModifier(mod string) {
	t := reflect.ValueOf(v)
	m := t.MethodByName(mod)
	v.updateValue = m
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

func (v *ViewData) COUNT(newData interface{}, groupByName string) error {
	if v.field.fieldType != INTEGER {
		return fmt.Errorf("not integer")
	}
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
