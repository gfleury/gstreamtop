package input

import (
	"gopkg.in/check.v1"
	"strings"
	// "testing"
	// "github.com/gfleury/gstreamtop/output"
)

func (s *Suite) TestLoopCSV(c *check.C) {
	reader := strings.NewReader("bla,bla,bla,bla,bla\n")
	s.csv.Run(reader)
	select {
	case err := <-*s.text.Errors():
		c.Assert(err, check.IsNil)
	default:
	}

}
