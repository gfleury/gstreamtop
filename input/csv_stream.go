package input

import (
	"encoding/csv"
	"io"
	"sync/atomic"

	"github.com/gfleury/gstreamtop/output"
	"github.com/gfleury/gstreamtop/tablestream"
)

type CSVStreamInput struct {
	Inputer
	table       tablestream.Table
	errors      *chan error
	inputExists int32
	entriesRead int64
}

func CreateCSVStreamInputFromStreamOutput(o output.Outputer) (Inputer, error) {
	i := &CSVStreamInput{}
	// i.stream = o.Stream()
	i.errors = o.ErrorChan()
	atomic.StoreInt32(&i.inputExists, 1)
	o.SetInputExists(i.InputExists)

	return i, nil
}

func (i *CSVStreamInput) Table() tablestream.Table {
	return i.table
}

func (i *CSVStreamInput) SetTable(t tablestream.Table) {
	i.table = t
}

func (i *CSVStreamInput) Configure() error {
	return nil
}

func (i *CSVStreamInput) PushData(data interface{}) {
	err := i.Table().AddRow(data)
	if err != nil {
		*i.Errors() <- err
	}
}

func (i *CSVStreamInput) Run(file io.Reader) {
	go i.Loop(file)
}

func (i *CSVStreamInput) Loop(file io.Reader) {
	reader := csv.NewReader(file)
	reader.Comma = ','
	for {
		entry, err := reader.Read()

		if err != nil {
			break
		}
		i.PushData(entry)
		i.entriesRead++
	}
	atomic.StoreInt32(&i.inputExists, 0)
}

func (i *CSVStreamInput) InputExists() *bool {
	cond := atomic.LoadInt32(&i.inputExists) == 1
	return &cond
}

func (i *CSVStreamInput) Errors() *chan error {
	return i.errors
}

func (i *CSVStreamInput) EntriesRead() int64 {
	return i.entriesRead
}
