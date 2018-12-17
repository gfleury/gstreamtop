package tablestream

import (
	"time"

	"gopkg.in/check.v1"
)

func (s *Suite) TestSetAggregatedValue(c *check.C) {
	vd := AggregatedViewData{
		SimpleViewData: SimpleViewData{
			field: &Field{
				name:      "field1",
				fieldType: INTEGER,
			},
		},
		data: make(map[string]interface{}),
	}

	vd.UpdateModifier("SetAggregatedValue")
	vd.CallUpdateValue(AggregatedValue{value: "10", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 10)
	vd.CallUpdateValue(AggregatedValue{value: "-", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 0)
	vd.CallUpdateValue(AggregatedValue{value: "20", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 20)

	vd.UpdateModifier("SUM")
	vd.CallUpdateValue(AggregatedValue{value: "20", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 40)
	vd.CallUpdateValue(AggregatedValue{value: "-", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 40)
	vd.CallUpdateValue(AggregatedValue{value: "50", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 90)

	vd.UpdateModifier("COUNT")
	vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 91)
	vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 92)
	vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 93)

	vd.UpdateModifier("MAX")
	vd.CallUpdateValue(AggregatedValue{value: "100", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 100)
	vd.CallUpdateValue(AggregatedValue{value: "110", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 110)
	vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 110)

	vd.UpdateModifier("MIN")
	vd.CallUpdateValue(AggregatedValue{value: "100", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 100)
	vd.CallUpdateValue(AggregatedValue{value: "110", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 100)
	vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey}})
	c.Check(vd.Value().(int), check.Equals, 90)

	vd.UpdateModifier("AVG")
	vd.CallUpdateValue(AggregatedValue{value: "90", groupBy: []string{lastItemKey + "avg"}})
	c.Check(vd.Fetch(lastItemKey+"avg").(AnalyticFunc).Value(), check.Equals, 90)
	vd.CallUpdateValue(AggregatedValue{value: "110", groupBy: []string{lastItemKey + "avg"}})
	c.Check(vd.Fetch(lastItemKey+"avg").(AnalyticFunc).Value(), check.Equals, 100)
	vd.CallUpdateValue(AggregatedValue{value: "200", groupBy: []string{lastItemKey + "avg"}})
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

	table, err := stream.GetTable("log")
	c.Assert(err, check.IsNil)

	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)
	err = table.AddRow(`92.115.179.247 - - [20/May/2015:21:05:35 +0000] "GET /favicon.ico HTTP/1.1" 200 366638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/20100101 Firefox/20.0"`)

	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)
	err = table.AddRow(`66.169.220.99 - - [20/May/2015:21:05:03 +0000] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0"`)

	err = table.AddRow(`180.76.6.130 - - [20/May/2015:20:05:43 +0000] "GET /robots.txt HTTP/1.1" 200 - "-" "Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2"`)
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
