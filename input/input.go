package input

import (
	"os"
	"sync/atomic"

	"github.com/gfleury/gstreamtop/output"
	"github.com/gfleury/gstreamtop/tablestream"
)

type Inputer interface {
	Loop()
	Configure() error
	PushData()
	Run()
	InputExists() *bool
}

type StreamInput struct {
	Inputer
	stream      *tablestream.Stream
	errors      *chan error
	inputExists int32
}

func CreateStreamInputFromStreamOutput(o output.Outputer) (*StreamInput, error) {
	i := &StreamInput{}
	i.stream = o.Stream()
	i.errors = o.ErrorChan()
	atomic.StoreInt32(&i.inputExists, 1)
	o.SetInputExists(i.InputExists)

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

func (i *StreamInput) InputExists() *bool {
	cond := atomic.LoadInt32(&i.inputExists) == 1
	return &cond
}
