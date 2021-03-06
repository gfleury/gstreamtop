package tablestream

import (
	"regexp"
	"strings"
	"time"
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
	Like           Operator = "like"
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
				var param interface{}
				if _, ok := c.left.(*AggregatedViewData); ok {
					param = AggregatedValue{value: value, groupBy: []string{lastItemKey}}
				} else {
					param = value
				}
				_, err := c.left.CallUpdateValue(param)
				if err != nil {
					return false
				}
			}
		}
		if rightField != nil {
			if rightField.name == key {
				var param interface{}
				if _, ok := c.right.(*AggregatedViewData); ok {
					param = AggregatedValue{value: value, groupBy: []string{lastItemKey}}
				} else {
					param = value
				}
				_, err := c.right.CallUpdateValue(param)
				if err != nil {
					return false
				}
			}
		}
	}

	switch c.operator {
	case Equal:
		return c.left.Value() == c.right.Value()
	case NotEqual:
		return c.left.Value() != c.right.Value()
	case Greater:
		switch c.left.VarType() {
		case INTEGER:
			return c.left.Value().(int) > c.right.Value().(int)
		case DATETIME:
			return c.left.Value().(time.Time).After(c.right.Value().(time.Time))
		}
	case GreaterOrEqual:
		return c.left.Value().(int) >= c.right.Value().(int)
	case Less:
		switch c.left.VarType() {
		case INTEGER:
			return c.left.Value().(int) < c.right.Value().(int)
		case DATETIME:
			return c.left.Value().(time.Time).Before(c.right.Value().(time.Time))
		}
	case LessOrEqual:
		return c.left.Value().(int) <= c.right.Value().(int)
	case Like:
		likeString := strings.Replace(c.right.Value().(string), "%", ".*", -1)
		likeRegexp := regexp.MustCompile(likeString)
		return likeRegexp.MatchString(c.left.Value().(string))
	}
	return false
}
