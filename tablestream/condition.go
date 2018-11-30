package tablestream

import (
	"strings"
)

type Conditioner interface {
	Evaluate(map[string]string) bool
}

type Operator string

const (
	Equal          Operator = "="
	NotEqual       Operator = "!="
	Greater        Operator = ">"
	GreaterOrEqual Operator = ">="
	Less           Operator = "<"
	LessOrEqual    Operator = "<="
	Like           Operator = "LIKE"
)

type SimpleCondition struct {
	Conditioner
	left     ViewData
	right    ViewData
	operator Operator
}

type OrCondition struct {
	Conditioner
	left  Conditioner
	right Conditioner
}

type AndCondition struct {
	Conditioner
	left  Conditioner
	right Conditioner
}

type ParentCondition struct {
	Conditioner
	condition Conditioner
}

func (c *OrCondition) Evaluate(row map[string]string) bool {
	return c.left.Evaluate(row) || c.right.Evaluate(row)
}

func (c *AndCondition) Evaluate(row map[string]string) bool {
	return c.left.Evaluate(row) && c.right.Evaluate(row)
}

func (c *ParentCondition) Evaluate(row map[string]string) bool {
	return c.condition.Evaluate(row)
}

func (c *SimpleCondition) Evaluate(row map[string]string) bool {

	for key, value := range row {
		leftField := c.left.Field()
		rightField := c.right.Field()

		if leftField != nil {
			if leftField.name == key {
				c.left.SetValue(value)
			}
		}
		if rightField != nil {
			if rightField.name == key {
				c.right.SetValue(value)
			}
		}
	}

	switch c.operator {
	case Equal:
		return c.left.Value() == c.right.Value()
	case NotEqual:
		return c.left.Value() != c.right.Value()
	case Greater:
		return c.left.Value().(int) > c.right.Value().(int)
	case GreaterOrEqual:
		return c.left.Value().(int) >= c.right.Value().(int)
	case Less:
		return c.left.Value().(int) < c.right.Value().(int)
	case LessOrEqual:
		return c.left.Value().(int) <= c.right.Value().(int)
	case Like:
		return strings.Contains(c.left.Value().(string), c.right.Value().(string))
	}
	return false
}
