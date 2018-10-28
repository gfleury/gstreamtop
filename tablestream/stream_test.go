package tablestream

import (
	"gopkg.in/check.v1"
)

func (s *Suite) TestGetTable(c *check.C) {
	table, err := s.stream.GetTable("user")

	c.Assert(err, check.IsNil)
	c.Assert(table.name, check.Equals, "user")
}

func (s *Suite) TestAddTable(c *check.C) {
	table := Table{name: "TableTest_Fake"}
	s.stream.AddTable(&table)
	tableGot, err := s.stream.GetTable("TableTest_Fake")

	c.Assert(err, check.IsNil)
	c.Assert(&table, check.Equals, tableGot)
}

func (s *Suite) TestAddView(c *check.C) {
	view := CreateView("testview", "gby")
	s.stream.AddView(view)

	viewGot, err := s.stream.GetView("testview")

	c.Assert(err, check.IsNil)
	c.Assert(view, check.Equals, viewGot)
}

func (s *Suite) TestGetView(c *check.C) {
	_, err := s.stream.GetView("user")

	c.Assert(err, check.NotNil)
}

func (s *Suite) TestQuerySelectWithoutGroupBy(c *check.C) {
	query := `SELECT shell FROM user;`
	err := s.stream.Query(query)

	c.Assert(err, check.ErrorMatches, "the query should have at least one GROUP BY, for filtering use grep")
}

func (s *Suite) TestQueryCreateTableWithoutRegexMapping(c *check.C) {
	query := `CREATE TABLE user(gid INTEGER, shell VARCHAR)
						LINES TERMINATED BY '\n';`
	err := s.stream.Query(query)

	c.Assert(err, check.ErrorMatches, "unable to find FIELDS IDENTIFIED by")
}

func (s *Suite) TestQueryCreateTableWithWrongRegexMapping(c *check.C) {
	query := `CREATE TABLE user(gid INTEGER, shell VARCHAR)
						FIELDS IDENTIFIED BY '(Invalidxxx regexppp?d>[09]+):.*:(l>.[^:]*)$'
						LINES TERMINATED BY '\n';`
	err := s.stream.Query(query)

	c.Assert(err, check.ErrorMatches, "regex present on FIELDS IDENTIFIED by failed to compile.*")
}

func (s *Suite) TestQueryCreateTableWithMissingFieldsFromRegex(c *check.C) {
	query := `CREATE TABLE user(gid INTEGER, shell VARCHAR)
						FIELDS IDENTIFIED BY '(?P<gid>[0-9]+):.*$'
						LINES TERMINATED BY '\n';`
	err := s.stream.Query(query)

	c.Assert(err, check.ErrorMatches, "regex groups doesn't match table columns: missing -1 field.*")
}

func (s *Suite) TestQueryCreateTableWithMissingFieldsFromTableFields(c *check.C) {
	query := `CREATE TABLE user(shell VARCHAR)
						FIELDS IDENTIFIED BY '(?P<gid>[0-9]+):.*:(?P<shell>.[^:]*)$'
						LINES TERMINATED BY '\n';`
	err := s.stream.Query(query)

	c.Assert(err, check.ErrorMatches, "regex groups doesn't match table columns: missing 1 field.*")
}
