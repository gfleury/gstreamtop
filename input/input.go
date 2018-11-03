package input

import (
	"os"

	"github.com/gfleury/gstreamtop/output"
	"github.com/gfleury/gstreamtop/tablestream"
)

type Inputer interface {
	Loop()
	Configure() error
	PushData()
	Run()
}

type StreamInput struct {
	Inputer
	stream *tablestream.Stream
	errors *chan error
}

func CreateStreamInputFromStreamOutput(o output.Outputer) (*StreamInput, error) {
	i := &StreamInput{}
	i.stream = o.Stream()
	i.errors = o.ErrorChan()
	return i, nil
}

func (i *StreamInput) PushData(data string) {
	tables := i.stream.GetTables()
	for _, table := range tables {
		table.AddRow(data)
	}
}

func (i *StreamInput) Run(file *os.File) {
	go i.Loop(file)
}
