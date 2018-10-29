package conf

import (
	"os"

	"gopkg.in/check.v1"
)

func (s *Suite) TestReadConfTest(c *check.C) {
	s.c.SetFileName("../mapping.yaml")
	err := s.c.ReadFile()
	c.Assert(err, check.IsNil)
	err = s.c.Write(os.Stdout)
	c.Assert(err, check.IsNil)
}
