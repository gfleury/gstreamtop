package tablestream

import (
	"gopkg.in/check.v1"
)

func (s *Suite) TestPrepareSelectGroupBy(c *check.C) {
	stream := Stream{}

	err := stream.Query(`CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
		dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
		response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR)
		WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
		LINES TERMINATED BY '\n';`)
	c.Assert(err, check.IsNil)

	queries := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url);",
		"SELECT url, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY url;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY urly;",
	}

	groupBy := []string{
		"URLIFY(url)",
		"url",
		"urly",
	}

	for i, query := range queries {
		err = stream.Query(query)
		c.Assert(err, check.IsNil)
		c.Assert(stream.GetViews()[i].groupByField.Name(), check.Equals, groupBy[i])
	}

	queriesFail := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY nonexistentColumun;",
		"SELECT NONEXISTENT(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY NONEXISTENT(url);",
	}

	queriesErrors := []string{
		"GROUP BY column nonexistentColumun not found",
		"function NONEXISTENT not found",
	}

	for i, query := range queriesFail {
		err = stream.Query(query)
		c.Assert(err, check.ErrorMatches, queriesErrors[i])
	}
}

func (s *Suite) TestPrepareCreate(c *check.C) {
	stream := Stream{}

	err := stream.Query("CREATE TABLE test(ip VARCHAR, col2 VARCHAR) WITH FIELDS IDENTIFIED BY '' LINES TERMINATED BY '\n';")
	c.Assert(err, check.ErrorMatches, "no FIELDS IDENTIFIED BY found.*")

	err = stream.Query("CREATE TABLE test(col2 VARCHAR, col1 VARCHAR) WITH FIELDS IDENTIFIED BY '^(?P<col2>\\S+)\\s(?P<col1>\\S+)$' LINES TERMINATED BY '\n';")
	c.Assert(err, check.IsNil)

	err = stream.Query("CREATE TABLE test(col2 VARCHAR, col1 VARCHAR) WITH FIELDS IDENTIFIED BY '^(?P<col2>\\S+) (?P<col1>\\S+)$' LINES TERMINATED BY '\n';")
	c.Assert(err, check.IsNil)
}

// ORDER BY column1, column2, ... ASC|DESC;
func (s *Suite) TestPrepareSelectOrderBy(c *check.C) {
	stream := Stream{}

	err := stream.Query(`CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
		dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
		response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR)
		WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
		LINES TERMINATED BY '\n';`)
	c.Assert(err, check.IsNil)

	queries := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url) ORDER BY COUNT(*);",
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url) ORDER BY SUM(size);",
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response), size FROM log GROUP BY URLIFY(url) ORDER BY log.size;",
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response), size FROM log GROUP BY URLIFY(url) ORDER BY log.size ASC;",
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response), size FROM log GROUP BY URLIFY(url) ORDER BY log.size DESC;",
	}

	orderBy := [][]string{
		{"COUNT(*)", "asc"},
		{"SUM(size)", "asc"},
		{"size", "asc"},
		{"size", "asc"},
		{"size", "desc"},
	}

	for i, query := range queries {
		err = stream.Query(query)
		c.Assert(err, check.IsNil)
		c.Assert(stream.GetViews()[i].orderBy.orderByField.Name(), check.Equals, orderBy[i][0])
		c.Assert(stream.GetViews()[i].orderBy.direction, check.Equals, orderBy[i][1])
	}

	queriesFail := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url) ORDER BY SUM(nonexistent);",
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url) ORDER BY NONEXISTENT(size);",
	}

	queriesErrors := []string{
		"ORDER BY column SUM.nonexistent. not found",
		"ORDER BY column NONEXISTENT.size. not found",
	}

	for i, query := range queriesFail {
		err = stream.Query(query)
		c.Assert(err, check.ErrorMatches, queriesErrors[i])
	}
}

// LIMIT
func (s *Suite) TestPrepareSelectLimit(c *check.C) {
	stream := Stream{}

	err := stream.Query(`CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
		dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
		response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR)
		WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
		LINES TERMINATED BY '\n';`)
	c.Assert(err, check.IsNil)

	queries := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY URLIFY(url) LIMIT 10;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log GROUP BY urly LIMIT 5;",
	}

	limits := []int{10, 5}

	for i, query := range queries {
		err = stream.Query(query)
		c.Assert(err, check.IsNil)
		c.Assert(stream.GetViews()[i].limit, check.Equals, limits[i])
	}

}

// WHERE
func (s *Suite) TestPrepareSelectWhere(c *check.C) {
	stream := Stream{}

	err := stream.Query(`CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
		dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
		response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR)
		WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
		LINES TERMINATED BY '\n';`)
	c.Assert(err, check.IsNil)

	queries := []string{
		"SELECT URLIFY(url), COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE URLIFY(url)='/favicon.ico' GROUP BY URLIFY(url);",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE urly!='/favicon.ico' GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE urly LIKE '/Bro%'  GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE size>500 GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE size>=500 GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE size<500 GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE size<=500 GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE size<=500 AND size>=100 GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE size>=500 OR size<=100 GROUP BY urly;",
		"SELECT URLIFY(url) as urly, COUNT(*), SUM(size), AVG(size), MAX(response) FROM log WHERE ((size>=500 OR size<=100) AND (response<500 OR response>=200)) OR urly LIKE '/salve%' GROUP BY urly;",
	}

	results := []bool{
		false,
		true,
		true,
		true,
		true,
		false,
		false,
		false,
		true,
		true,
	}

	row := map[string]string{
		"ip":        "127.0.0.1",
		"col2":      "-",
		"col3":      "-",
		"dt":        "12/12/2019",
		"method":    "GET",
		"url":       "/Broooz?param=george",
		"version":   "HTTP/1.1",
		"response":  "200",
		"size":      "1024",
		"col10":     "-",
		"useragent": "Curl 1.0",
	}

	for i, query := range queries {
		err = stream.Query(query)
		c.Assert(stream.GetViews()[i].evaluateWhere(row), check.Equals, results[i])
		c.Assert(err, check.IsNil)
	}

}
