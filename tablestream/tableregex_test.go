package tablestream

import (
	"regexp"
	"sync"

	"gopkg.in/check.v1"
)

func (s *Suite) TestTableCreateTableRegex(c *check.C) {
	table := CreateTableRegex("testTable")

	c.Assert(table, check.DeepEquals, &TableRegex{
		TableSimple: TableSimple{
			name:         "testTable",
			typeInstance: make(map[string]chan map[string]string),
		},
	})
}

func (s *Suite) TestAddRow(c *check.C) {

	table := CreateTableRegex("testTable")

	table.AddField(&Field{name: "f1", fieldType: VARCHAR})
	table.AddField(&Field{name: "f2", fieldType: VARCHAR})
	table.AddField(&Field{name: "f3", fieldType: VARCHAR})
	table.AddField(&Field{name: "f4", fieldType: INTEGER})

	table.fieldRegexMap = regexp.MustCompile(`(?P<f1>\w+),(?P<f2>\w+),(?P<f3>\w+),(?P<f4>\w+).*$`)

	table.typeInstance["view1"] = make(chan map[string]string)

	var msg map[string]string
	var mmutex sync.Mutex

	go func() {
		mmutex.Lock()
		defer mmutex.Unlock()
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

	mmutex.Lock()
	defer mmutex.Unlock()

	c.Assert(msg, check.DeepEquals, map[string]string{
		"f1": "name1",
		"f2": "surname1",
		"f3": "surname11",
		"f4": "1010",
	})
}
