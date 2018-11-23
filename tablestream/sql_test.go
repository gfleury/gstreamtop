package tablestream

import (
	"gopkg.in/check.v1"
)

func (s *Suite) TestPrepareSelectGroupBy(c *check.C) {
	stream := Stream{}

	err := stream.Query(`CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
		dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
		response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR)
		FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
		LINES TERMINATED BY '\n';`)
	c.Assert(err, check.IsNil)

	queries := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url);",
		"SELECT url, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY url;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY urly;",
	}

	for _, query := range queries {
		err = stream.Query(query)
		c.Assert(err, check.IsNil)
	}

	queriesFail := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY nonexistentColumun;",
		"SELECT NONEXISTENT(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY NONEXISTENT(url);",
	}

	queriesErrors := []string{
		"GROUP BY column not found",
		"function NONEXISTENT not found",
	}

	for i, query := range queriesFail {
		err = stream.Query(query)
		c.Assert(err, check.ErrorMatches, queriesErrors[i])
	}
}

func (s *Suite) TestPrepareCreate(c *check.C) {
	stream := Stream{}

	err := stream.Query("CREATE TABLE test(ip VARCHAR, col2 VARCHAR) FIELDS IDENTIFIED BY '' LINES TERMINATED BY '\n';")
	c.Assert(err, check.ErrorMatches, "no FIELDS IDENTIFIED BY found")

	err = stream.Query("CREATE TABLE test(col2 VARCHAR, col1 VARCHAR) FIELDS IDENTIFIED BY '^(?P<col2>\\S+)\\s(?P<col1>\\S+)$' LINES TERMINATED BY '\n';")
	c.Assert(err, check.IsNil)
}
