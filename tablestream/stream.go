package tablestream

import (
	"fmt"
)

type Stream struct {
	tables []*Table
	views  []*View
}

func (s *Stream) GetTable(name string) (*Table, error) {
	for i, table := range s.tables {
		if table.name == name {
			return s.tables[i], nil
		}
	}
	return nil, fmt.Errorf("no table named %s", name)
}

func (s *Stream) AddTable(t *Table) {
	s.tables = append(s.tables, t)
}

func (s *Stream) AddView(v *View) {
	s.views = append(s.views, v)
}

func (s *Stream) GetView(name string) (*View, error) {
	for i, view := range s.views {
		if view.name == name {
			return s.views[i], nil
		}
	}
	return nil, fmt.Errorf("no view named %s", name)
}
