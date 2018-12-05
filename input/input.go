package input

import (
	"io"

	"github.com/gfleury/gstreamtop/tablestream"
)

type Inputer interface {
	Configure() error
	PushData(interface{})
	Run(io.Reader)
	InputExists() *bool
	Table() tablestream.Table
	SetTable(tablestream.Table)
	Errors() *chan error
	EntriesRead() int64
}
