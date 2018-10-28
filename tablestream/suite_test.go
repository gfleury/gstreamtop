package tablestream

import (
	"testing"
	"time"

	"gopkg.in/check.v1"
)

type Suite struct {
	stream *Stream
}

func (s *Suite) SetUpSuite(c *check.C) {
	query := `CREATE TABLE user(gid INTEGER, shell VARCHAR)
		 	      FIELDS IDENTIFIED BY '(?P<gid>[0-9]+):.*:(?P<shell>.[^:]*)$'
						LINES TERMINATED BY '\n';`

	s.stream = &Stream{}

	err := s.stream.Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT shell, COUNT(*), SUM(gid)
					FROM user
					GROUP BY shell;`

	err = s.stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := s.stream.GetTable("user")
	c.Assert(err, check.IsNil)

	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/bin")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/bin")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/bin")

	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")

	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/true")
	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(500 * time.Millisecond)
}

func (s *Suite) TearDownSuite(c *check.C) {
}

var _ = check.Suite(&Suite{})

func Test(t *testing.T) {
	check.TestingT(t)
}

func (s *Suite) TestQuery(c *check.C) {
	allRows := s.stream.views[0].GetAllRows()
	c.Assert(allRows[1][0], check.Equals, "/usr/bin/bin")
	c.Assert(allRows[1][1], check.Equals, "3")
	c.Assert(allRows[1][2], check.Equals, "789")

	c.Assert(allRows[2][0], check.Equals, "/usr/bin/false")
	c.Assert(allRows[2][1], check.Equals, "5")
	c.Assert(allRows[2][2], check.Equals, "1315")

	c.Assert(allRows[3][0], check.Equals, "/usr/bin/true")
	c.Assert(allRows[3][1], check.Equals, "1")
	c.Assert(allRows[3][2], check.Equals, "263")

	s.stream.views[0].PrintView()
}
