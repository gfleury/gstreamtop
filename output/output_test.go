package output

import (
	"github.com/gfleury/gstreamtop/tablestream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	check "gopkg.in/check.v1"
)

type Suite struct {
	o Outputer
}

func (s *Suite) SetUpSuite(c *check.C) {
}

func (s *Suite) TearDownSuite(c *check.C) {
}

var _ = check.Suite(&Suite{})

func Test(t *testing.T) {
	check.TestingT(t)
}

func (s *Suite) TestPrometheus(c *check.C) {
	output := &PrometheusOutput{}
	output.stream = &tablestream.Stream{}

	s.o = output

	err := s.o.Configure()
	c.Assert(err, check.IsNil)

	query := `CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR,
		dt VARCHAR, method VARCHAR, url VARCHAR, version VARCHAR, 
		response INTEGER, size INTEGER, col10 VARCHAR, useragent VARCHAR) 
		WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<col10>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
		LINES TERMINATED BY '\n';`

	err = s.o.Stream().Query(query)
	c.Assert(err, check.IsNil)

	query = `SELECT URLIFY(url) as url, COUNT(*) as count, SUM(size) as sum, size, MAX(response)
		FROM log GROUP BY url, size ORDER BY count, size LIMIT 20;`

	err = s.o.Stream().Query(query)
	c.Assert(err, check.IsNil)

	table, err := s.o.Stream().Table("log")
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

	output.createMetrics()
	http.Handle("/metrics", promhttp.Handler())
	output.publishMetrics()

	request, err := http.NewRequest("GET", "/metrics", nil)
	c.Assert(err, check.IsNil)
	recorder := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(recorder, request)
	c.Assert(recorder.Code, check.Equals, http.StatusOK)
	body, err := ioutil.ReadAll(recorder.Body)
	c.Assert(err, check.IsNil)
	strBody := string(body)
	if !strings.Contains(strBody, `FirstView_count{row="/favicon.ico/3638"} 3.0`) ||
		!strings.Contains(strBody, `FirstView_count{row="/favicon.ico/366638"} 3.0`) {
		c.FailNow()
	}

}
