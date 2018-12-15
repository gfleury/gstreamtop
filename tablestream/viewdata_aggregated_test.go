package tablestream

import (
	"time"

	"gopkg.in/check.v1"
)

func (s *Suite) TestGroupBYExecution(c *check.C) {
	query := `CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
    dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
    response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR) 
    WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
    LINES TERMINATED BY '\n';`

	stream := &Stream{}

	err := stream.Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT URLIFY(url) as url, COUNT(*) as count, 
		SUM(size) as sum, size, MAX(response) FROM log 
		GROUP BY url, size ORDER BY count LIMIT 20;`

	err = stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := stream.GetTable("log")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)

	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)

	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(500 * time.Millisecond)

	allRows := stream.views[0].FetchAllRows()
	// +--------------+-------+---------+--------+---------------+
	// |     URL      | COUNT |   SUM   |  SIZE  | MAX(RESPONSE) |
	// +--------------+-------+---------+--------+---------------+
	// | /favicon.ico |     3 | 1099914 | 366638 |           200 |
	// | /favicon.ico |     3 |   10914 |   3638 |           200 |
	// +--------------+-------+---------+--------+---------------+
	c.Assert(allRows[1][0], check.Equals, "/favicon.ico")
	c.Assert(allRows[1][1], check.Equals, "3")
	c.Assert(allRows[1][2], check.Equals, "1099914")
	c.Assert(allRows[1][3], check.Equals, "366638")
	c.Assert(allRows[1][4], check.Equals, "200")

	c.Assert(allRows[2][0], check.Equals, "/favicon.ico")
	c.Assert(allRows[2][1], check.Equals, "3")
	c.Assert(allRows[2][2], check.Equals, "10914")
	c.Assert(allRows[2][3], check.Equals, "3638")
	c.Assert(allRows[2][4], check.Equals, "200")

	s.stream.views[0].PrintView()
}
