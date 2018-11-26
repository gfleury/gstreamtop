package tablestream

import (
	"fmt"

	"gopkg.in/check.v1"
)

func (s *Suite) TestAddError(c *check.C) {
	view := View{}
	view.AddError(fmt.Errorf("Test error"))

	c.Assert(view.errors[0], check.ErrorMatches, "Test error")
}
