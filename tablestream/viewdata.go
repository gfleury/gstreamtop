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
	SelectedField() bool
	SetParams(params []interface{})
}

type SimpleViewData struct {
	ViewData
	name          string
	value         interface{}
	field         *Field
	updateValue   reflect.Value
	varType       fieldType
	selectedField bool
	params        []interface{}
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
	switch v.VarType() {
	case INTEGER:
		var strValue string
		if aggValue, ok := value.(AggregatedValue); ok {
			strValue = aggValue.value.(string)
		} else {
			strValue = value.(string)
		}
		v.value, err = strconv.Atoi(strValue)
	case DATETIME:
		strValue := value.(string)
		v.value, err = parseDate(strValue)
	case VARCHAR:
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
	return nil
}

func (v *SimpleViewData) CallUpdateValue(value interface{}) (interface{}, error) {
	result := v.updateValue.Call([]reflect.Value{reflect.ValueOf(value)})
	err := result[1].Interface()
	if err != nil {
		return result[0].Interface(), err.(error)
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

func (v *SimpleViewData) SelectedField() bool {
	return v.selectedField
}

func (v *SimpleViewData) SetParams(params []interface{}) {
	v.params = params
}
