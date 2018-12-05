package input

import (
	"github.com/gfleury/gstreamtop/tablestream"
	"gopkg.in/check.v1"
	"testing"

	"github.com/gfleury/gstreamtop/output"
)

type Suite struct {
	csv  Inputer
	text Inputer
	o    output.Outputer
}

func (s *Suite) SetUpSuite(c *check.C) {
	var err error

	textTable := &tablestream.TableSimple{}
	textTable.SetRowSeparator("\n")

	s.o = &output.SimpleTableOutput{}
	s.csv, err = CreateCSVStreamInputFromStreamOutput(s.o)
	c.Assert(err, check.IsNil)
	s.csv.SetTable(&tablestream.TableSimple{})

	s.text, err = CreateStreamInputFromStreamOutput(s.o)
	c.Assert(err, check.IsNil)
	s.text.SetTable(textTable)
}

func (s *Suite) TearDownSuite(c *check.C) {
}

var _ = check.Suite(&Suite{})

func Test(t *testing.T) {
	check.TestingT(t)
}
