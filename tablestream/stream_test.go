package tablestream

import (
	"fmt"
	"testing"

	"gopkg.in/check.v1"
)

type Suite struct{}

var _ = check.Suite(Suite{})

func Test(t *testing.T) {
	check.TestingT(t)
}

func (Suite) TestQuery(c *check.C) {
	query := `CREATE TABLE user(gid INTEGER, shell VARCHAR)
		 	      FIELDS IDENTIFIED BY '(?P<gid>[0-9]+):.*:(?P<shell>.[^:]*)$'
						LINES TERMINATED BY '\n';`

	stream := Stream{}

	err := stream.Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT shell, COUNT(*), SUM(gid)
					FROM user
					GROUP BY shell;`

	err = stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := stream.GetTable("user")
	c.Assert(err, check.IsNil)

	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/false")

	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/true")

	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/bin")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/bin")
	err = table.AddRow("_analyticsd:*:263:263:Analytics Daemon:/var/db/analyticsd:/usr/bin/bin")

	c.Assert(err, check.IsNil)

	res := stream.views[0].GetStringViewData(0)
	fmt.Println(res)
	resS := stream.views[0].GetIntViewData(1)
	fmt.Println(resS)
	resS = stream.views[0].GetIntViewData(2)
	fmt.Println(resS)
}
