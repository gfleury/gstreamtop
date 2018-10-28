package tablestream

import (
	"fmt"

	"gopkg.in/check.v1"
)

func (s *Suite) TestAddError(c *check.C) {
	field := Field{}
	field.AddError(fmt.Errorf("Test error"))

	c.Assert(field.errors[0], check.ErrorMatches, "Test error")
}
