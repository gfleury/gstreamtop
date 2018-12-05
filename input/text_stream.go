package input

import (
	"bufio"
	"io"
	"strings"
	"sync/atomic"

	"github.com/gfleury/gstreamtop/output"
	"github.com/gfleury/gstreamtop/tablestream"
)

type StreamInput struct {
	Inputer
	table       tablestream.Table
	errors      *chan error
	inputExists int32
	entriesRead int64
}

func CreateStreamInputFromStreamOutput(o output.Outputer) (Inputer, error) {
	i := &StreamInput{}
	// i.stream = o.Stream()
	i.errors = o.ErrorChan()
	atomic.StoreInt32(&i.inputExists, 1)
	o.SetInputExists(i.InputExists)

	return i, nil
}

func (i *StreamInput) Configure() error {
	return nil
}

func (i *StreamInput) Table() tablestream.Table {
	return i.table
}

func (i *StreamInput) SetTable(t tablestream.Table) {
	i.table = t
}

func (i *StreamInput) PushData(data interface{}) {
	i.Table().AddRow(data)
}

func (i *StreamInput) Run(file io.Reader) {
	go i.Loop(file)
}

func (i *StreamInput) InputExists() *bool {
	cond := atomic.LoadInt32(&i.inputExists) == 1
	return &cond
}

func (i *StreamInput) Loop(file io.Reader) {
	var buf string
	reader := bufio.NewReader(file)
	table := i.Table()
	sep := table.RowSeparator()
	i.entriesRead = 0
	for {
		fread, err := reader.ReadString(sep[0])
		buf += fread
		if strings.Contains(buf, sep) {
			token := strings.Split(buf, sep)
			i.PushData(token[0])
			buf = token[1]
		}
		if err != nil {
			break
		}
		i.entriesRead++
	}
	atomic.StoreInt32(&i.inputExists, 0)
}

func (i *StreamInput) Errors() *chan error {
	return i.errors
}

func (i *StreamInput) EntriesRead() int64 {
	return i.entriesRead
}
