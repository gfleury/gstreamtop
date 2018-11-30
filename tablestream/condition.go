package tablestream

import (
	"strings"
)

type Conditioner interface {
	Evaluate() bool
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
	left     *ViewData
	right    *ViewData
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

func (c *OrCondition) Evaluate() bool {
	return c.left.Evaluate() || c.right.Evaluate()
}

func (c *AndCondition) Evaluate() bool {
	return c.left.Evaluate() && c.right.Evaluate()
}

func (c *ParentCondition) Evaluate() bool {
	return c.condition.Evaluate()
}

func (c *SimpleCondition) Evaluate() bool {
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
