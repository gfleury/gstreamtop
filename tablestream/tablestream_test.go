package tablestream

import (
	"regexp"
	"time"

	"gopkg.in/check.v1"
)

func (s *Suite) TestTableCreateTable(c *check.C) {
	table := CreateTable("testTable")

	c.Assert(table, check.DeepEquals, &Table{name: "testTable", typeInstance: make(map[string]chan map[string]string)})
}

func (s *Suite) TestAddField(c *check.C) {
	table := CreateTable("testTable")

	table.AddField(&Field{name: "f1", fieldType: INTEGER})
	table.AddField(&Field{name: "f2", fieldType: VARCHAR})

	c.Assert(table, check.DeepEquals,
		&Table{
			name:         "testTable",
			typeInstance: make(map[string]chan map[string]string),
			fields: []*Field{
				{name: "f1", fieldType: INTEGER},
				{name: "f2", fieldType: VARCHAR},
			},
		})

	field := table.GetField("f1")
	c.Assert(field, check.DeepEquals, &Field{name: "f1", fieldType: INTEGER})
}

func (s *Suite) TestAddRow(c *check.C) {

	table := CreateTable("testTable")

	table.AddField(&Field{name: "f1", fieldType: VARCHAR})
	table.AddField(&Field{name: "f2", fieldType: VARCHAR})
	table.AddField(&Field{name: "f3", fieldType: VARCHAR})
	table.AddField(&Field{name: "f4", fieldType: INTEGER})

	table.fieldRegexMap = regexp.MustCompile(`(?P<f1>\w+),(?P<f2>\w+),(?P<f3>\w+),(?P<f4>\w+).*$`)

	table.typeInstance["view1"] = make(chan map[string]string)

	var msg map[string]string

	go func() {
		for {
			for _, j := range table.typeInstance {
				select {
				case msg = <-j:
					return
				}
			}
		}

	}()

	table.AddRow("name1,surname1,surname11,1010,1111,3333, blew")

	for msg == nil {
		time.Sleep(time.Second)
	}

	c.Assert(msg, check.DeepEquals, map[string]string{
		"f1": "name1",
		"f2": "surname1",
		"f3": "surname11",
		"f4": "1010",
	})
}
