package tablestream

import (
	"fmt"
	"reflect"
	"strconv"
)

type ViewData interface {
	UpdateModifier(mod string) error
	Value() interface{}
	SetValue(value interface{}) (interface{}, error)
	Name() string
	SetName(string)
	Field() *Field
	CallUpdateValue(value interface{}) (interface{}, error)
	Length() int
	Fetch(key string) interface{}
	VarType() fieldType
	KeyArray() []kv
}

type SimpleViewData struct {
	ViewData
	name        string
	value       interface{}
	field       *Field
	table       *Table
	updateValue reflect.Value
	modifier    string
	varType     fieldType
}

func (v *SimpleViewData) VarType() fieldType {
	return v.varType
}

func (v *SimpleViewData) Name() string {
	return v.name
}

func (v *SimpleViewData) SetName(name string) {
	v.name = name
}

func (v *SimpleViewData) Field() *Field {
	return v.field
}

func (v *SimpleViewData) SetValue(value interface{}) (interface{}, error) {
	var err error
	if v.VarType() == INTEGER {
		var strValue string
		if aggValue, ok := value.(AggregatedValue); ok {
			strValue = aggValue.value.(string)
		} else {
			strValue = value.(string)
		}
		v.value, err = strconv.Atoi(strValue)
	} else {
		v.value = value
	}
	return v.value, err
}

func (v *SimpleViewData) Value() interface{} {
	return v.value
}

func (v *SimpleViewData) UpdateModifier(mod string) error {
	if v.field != nil {
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

func (v *SimpleViewData) CallUpdateValue(value interface{}) (interface{}, error) {
	result := v.updateValue.Call([]reflect.Value{reflect.ValueOf(value)})
	err := result[1].Interface()
	if err != nil {
		return nil, err.(error)
	}
	return result[0].Interface(), nil
}

func (v *SimpleViewData) Length() int {
	// if v.value != nil {
	// 	return 1
	// }
	return 0
}

func (v *SimpleViewData) Fetch(key string) interface{} {
	return v.value
}
