package tablestream

import (
	"time"

	check "gopkg.in/check.v1"
)

func (s *Suite) TestSetAggregatedValue(c *check.C) {
	vd := AggregatedViewData{
		SimpleViewData: SimpleViewData{
			field: &Field{
				name:      "field1",
				fieldType: INTEGER,
			},
			value: make(map[string]interface{}),
		},
	}

	err := vd.UpdateModifier("SetAggregatedValue")
	c.Check(err, check.IsNil)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "10", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 10)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "-", groupBy: []string{lastItemKey}})
	c.Check(err, check.ErrorMatches, "strconv.Atoi: parsing.*")
	c.Check(vd.Value().(int), check.Equals, 0)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "20", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 20)

	err = vd.UpdateModifier("SUM")
	c.Check(err, check.IsNil)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "20", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 40)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "-", groupBy: []string{lastItemKey}})
	c.Check(err, check.ErrorMatches, "strconv.Atoi: parsing.*")
	c.Check(vd.Value().(int), check.Equals, 40)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "50", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 90)

	err = vd.UpdateModifier("COUNT")
	c.Check(err, check.IsNil)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 91)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 92)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 93)

	err = vd.UpdateModifier("MAX")
	c.Check(err, check.IsNil)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "100", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 100)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "110", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 110)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 110)

	err = vd.UpdateModifier("MIN")
	c.Check(err, check.IsNil)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "100", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 100)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "110", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 100)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(err, check.IsNil)
	c.Check(vd.Value().(int), check.Equals, 90)

	err = vd.UpdateModifier("AVG")
	c.Check(err, check.IsNil)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey + "avg"}})
	c.Check(err, check.IsNil)
	c.Check(vd.Fetch(lastItemKey+"avg").(AnalyticFunc).Value(), check.Equals, 90)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "110", groupBy: []string{lastItemKey + "avg"}})
	c.Check(err, check.IsNil)
	c.Check(vd.Fetch(lastItemKey+"avg").(AnalyticFunc).Value(), check.Equals, 100)
	_, err = vd.CallUpdateValue(AggregatedValue{value: "200", groupBy: []string{lastItemKey + "avg"}})
	c.Check(err, check.IsNil)
	c.Check(vd.Fetch(lastItemKey+"avg").(AnalyticFunc).Value(), check.Equals, 133)

}

func (s *Suite) TestMergeKv(c *check.C) {
	input := []kv{kv{Key: "first", Value: []interface{}{10}}}
	expected := input
	result := []kv{}
	mergeKv(&result, &input, 0)
	c.Assert(result, check.DeepEquals, expected)
	mergeKv(&result, &input, 0)
	c.Assert(result, check.DeepEquals, []kv{kv{Key: "first", Value: []interface{}{10, 10}}})

	secondInput := []kv{kv{Key: "second", Value: []interface{}{20}}}
	mergeKv(&result, &secondInput, 0)
	c.Assert(result, check.DeepEquals, []kv{
		kv{Key: "first", Value: []interface{}{10, 10}},
		kv{Key: "second", Value: []interface{}{20}},
	})
	mergeKv(&result, &secondInput, 0)
	c.Assert(result, check.DeepEquals, []kv{
		kv{Key: "first", Value: []interface{}{10, 10}},
		kv{Key: "second", Value: []interface{}{20, 20}},
	})

	thirdInput := []kv{kv{Key: "third", Value: []interface{}{30}}}
	mergeKv(&result, &thirdInput, 0)
	c.Assert(result, check.DeepEquals, []kv{
		kv{Key: "first", Value: []interface{}{10, 10}},
		kv{Key: "second", Value: []interface{}{20, 20}},
		kv{Key: "third", Value: []interface{}{30}},
	})
	mergeKv(&result, &thirdInput, 0)
	c.Assert(result, check.DeepEquals, []kv{
		kv{Key: "first", Value: []interface{}{10, 10}},
		kv{Key: "second", Value: []interface{}{20, 20}},
		kv{Key: "third", Value: []interface{}{30, 30}},
	})
}

func (s *Suite) TestGroupByNameKeyString(c *check.C) {
	input := []string{"first", "second"}

	c.Assert(groupByNameKeyString(input), check.Equals, "first/second")
}

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

	table, err := stream.Table("log")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 - "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 - "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(1000 * time.Millisecond)

	allRows := stream.views[0].FetchAllRows()
	// +--------------+-------+---------+--------+---------------+
	// |     URL      | COUNT |   SUM   |  SIZE  | MAX(RESPONSE) |
	// +--------------+-------+---------+--------+---------------+
	// | /favicon.ico |     3 | 1099914 | 366638 |           200 |
	// | /favicon.ico |     3 |   10914 |   3638 |           200 |
	// .....
	// +--------------+-------+---------+--------+---------------+
	c.Assert(len(allRows), check.Equals, 4)
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

}

func (s *Suite) TestOrderBYExecution(c *check.C) {
	query := `CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
    dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
    response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR) 
    WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
    LINES TERMINATED BY '\n';`

	stream := &Stream{}

	err := stream.Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT URLIFY(url) as url, COUNT(*) as count, SUM(size) as sum, size, MAX(response)
	FROM log GROUP BY url, size ORDER BY count, size LIMIT 20;`

	err = stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := stream.Table("log")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(1000 * time.Millisecond)

	allRows := stream.views[0].FetchAllRows()
	// +--------------+-------+---------+--------+---------------+
	// |     URL      | COUNT |   SUM   |  SIZE  | MAX(RESPONSE) |
	// +--------------+-------+---------+--------+---------------+
	// | /favicon.ico |     3 | 1099914 | 366638 |           200 |
	// | /favicon.ico |     3 |   10914 |   3638 |           200 |
	// +--------------+-------+---------+--------+---------------+
	c.Assert(len(allRows), check.Equals, 3)
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

}

func (s *Suite) TestGroupBYWindowsExecution(c *check.C) {
	query := `CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
    dt DATETIME, method VARCHAR, url VARCHAR, version VARCHAR, 
    response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR) 
    WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
    LINES TERMINATED BY '\n';`

	stream := &Stream{}

	err := stream.Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT URLIFY(url) as url, COUNT(*) as count,
		SESSION(dt, 5, 'SECONDS') as tumbling, SUM(size) as sum, 
		size, MAX(response) FROM log 
		GROUP BY url, size ORDER BY count LIMIT 20;`

	err = stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := stream.Table("log")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 - "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 - "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(1000 * time.Millisecond)

	allRows := stream.views[0].FetchAllRows()
	// +--------------+-------+---------+--------+---------------+
	// |     URL      | COUNT |   SUM   |  SIZE  | MAX(RESPONSE) |
	// +--------------+-------+---------+--------+---------------+
	// | /favicon.ico |     3 | 1099914 | 366638 |           200 |
	// | /favicon.ico |     3 |   10914 |   3638 |           200 |
	// .....
	// +--------------+-------+---------+--------+---------------+
	c.Assert(len(allRows), check.Equals, 4)
	c.Assert(allRows[1][0], check.Equals, "/favicon.ico")
	c.Assert(allRows[1][1], check.Equals, "3")
	c.Assert(allRows[1][2], check.Equals, "Wed May 20 21:05:35 2015")
	c.Assert(allRows[1][3], check.Equals, "1099914")
	c.Assert(allRows[1][4], check.Equals, "366638")
	c.Assert(allRows[1][5], check.Equals, "200")

	c.Assert(allRows[2][0], check.Equals, "/favicon.ico")
	c.Assert(allRows[2][1], check.Equals, "3")
	c.Assert(allRows[2][2], check.Equals, "Wed May 20 21:05:03 2015")
	c.Assert(allRows[2][3], check.Equals, "10914")
	c.Assert(allRows[2][4], check.Equals, "3638")
	c.Assert(allRows[2][5], check.Equals, "200")

}

func (s *Suite) TestSingleQuotesRegex(c *check.C) {
	query := `CREATE TABLE nonAuth(reason VARCHAR, uri VARCHAR, referer VARCHAR, clientIp VARCHAR, userAgent VARCHAR) WITH FIELDS IDENTIFIED BY '^{\\Dreason\\D: \\D(?P<reason>[^\\\']*)\\D, \\Duri\\D: \\D(?P<uri>[^\\\']*)\\D, \\Dreferer\\D: \\D(?P<referer>[^\\\']*)\\D, \\DclientIp\\D: \\D(?P<clientIp>[^\\\']*)\\D, \\Duser_agent\\D: \\D(?P<userAgent>[^\\\']*)\\D}$' LINES TERMINATED BY '\n';`

	stream := &Stream{}

	err := stream.Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT reason, uri, COUNT(*) as count, referer, clientIp, userAgent from
	nonAuth GROUP BY uri, reason ORDER BY count ASC;`

	err = stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := stream.Table("nonAuth")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`{'reason': 'No cookie', 'uri': '/config', 'referer': 'https://example.com', 'clientIp': '33.44.44.6', 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15'}`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`{'reason': 'Invalid SSO cookie signature', 'uri': '/chair', 'referer': 'https://example.com', 'clientIp': '33.44.44.6', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'}`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`{'reason': 'Invalid SSO cookie signature', 'uri': '/bleom/sdsds', 'referer': 'https://example.com', 'clientIp': '33.44.44.6', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko'}`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`{'reason': 'Invalid SSO cookie signature', 'uri': '/config', 'referer': 'https://example.com', 'clientIp': '33.44.44.4', 'user_agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko'}`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`{'reason': 'Invalid SSO cookie signature', 'uri': '/bleom/sdsds', 'referer': 'https://example.com', 'clientIp': '33.44.44.4', 'user_agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko'}`)
	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(1000 * time.Millisecond)

	allRows := stream.views[0].FetchAllRows()

	c.Assert(len(allRows), check.Equals, 5)

}

func (s *Suite) TestTwoRunningQueriesWithGroupBYExecution(c *check.C) {
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

	query = `SELECT url, COUNT(*) as count, SUM(size) as sum, AVG(size), MAX(response)
	FROM log WHERE url LIKE '/robots%' AND response > 100 GROUP BY url ORDER BY
	count LIMIT 5;`

	err = stream.Query(query)
	c.Assert(err, check.IsNil)

	table, err := stream.Table("log")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	c.Assert(err, check.IsNil)

	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 102 "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
	c.Assert(err, check.IsNil)
	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 102 "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
	c.Assert(err, check.IsNil)

	// Time to flush the channels
	time.Sleep(1000 * time.Millisecond)

	allRows := stream.views[0].FetchAllRows()
	// +--------------+-------+---------+--------+---------------+
	// |     URL      | COUNT |   SUM   |  SIZE  | MAX(RESPONSE) |
	// +--------------+-------+---------+--------+---------------+
	// | /favicon.ico |     3 | 1099914 | 366638 |           200 |
	// | /favicon.ico |     3 |   10914 |   3638 |           200 |
	// .....
	// +--------------+-------+---------+--------+---------------+
	c.Assert(len(allRows), check.Equals, 4)
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

	// SECOND QUERY

	allRows = stream.views[1].FetchAllRows()
	// +--------------+-------+---------+--------+---------------+
	// |     URL      | COUNT |   SUM   |  SIZE  | MAX(RESPONSE) |
	// +--------------+-------+---------+--------+---------------+
	// | /favicon.ico |     3 | 1099914 | 366638 |           200 |
	// | /favicon.ico |     3 |   10914 |   3638 |           200 |
	// .....
	// +--------------+-------+---------+--------+---------------+
	c.Assert(len(allRows), check.Equals, 3)
	c.Assert(allRows[1][0], check.Equals, "/robots.txt")
	c.Assert(allRows[1][1], check.Equals, "2")
	c.Assert(allRows[1][2], check.Equals, "204")
	c.Assert(allRows[1][3], check.Equals, "102")
	c.Assert(allRows[1][4], check.Equals, "200")
}
