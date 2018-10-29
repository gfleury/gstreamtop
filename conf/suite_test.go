package conf

import (
	"testing"

	"gopkg.in/check.v1"
)

type Suite struct {
	c Configuration
}

func (s *Suite) SetUpSuite(c *check.C) {
}

func (s *Suite) TearDownSuite(c *check.C) {
}

var _ = check.Suite(&Suite{})

func Test(t *testing.T) {
	check.TestingT(t)
}
