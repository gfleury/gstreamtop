package tablestream

import (
	"gopkg.in/check.v1"
)

func (s *Suite) TestTableCreateTableSimple(c *check.C) {
	table := CreateTable("testTable")

	c.Assert(table, check.DeepEquals, &TableSimple{name: "testTable", typeInstance: make(map[string]chan map[string]string)})
}

func (s *Suite) TestAddField(c *check.C) {
	table := CreateTable("testTable")

	table.AddField(&Field{name: "f1", fieldType: INTEGER})
	table.AddField(&Field{name: "f2", fieldType: VARCHAR})

	c.Assert(table, check.DeepEquals,
		&TableSimple{
			name:         "testTable",
			typeInstance: make(map[string]chan map[string]string),
			fields: []*Field{
				{name: "f1", fieldType: INTEGER},
				{name: "f2", fieldType: VARCHAR},
			},
		})

	field := table.Field("f1")
	c.Assert(field, check.DeepEquals, &Field{name: "f1", fieldType: INTEGER})
}
