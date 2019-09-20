package output

import (
	"fmt"
	"github.com/gfleury/gstreamtop/conf"
	"github.com/gfleury/gstreamtop/profiling"
	"github.com/gfleury/gstreamtop/tablestream"
	"time"
)

type Outputer interface {
	Loop()
	Configure() error
	Stream() *tablestream.Stream
	ErrorChan() *chan error
	CreateStreamFromConfigurationMapping(mapping *conf.Mapping, createNamedQueries []string) error
	InputExists() *bool
	SetInputExists(func() *bool)
	EnableProfile()
	Profile() bool
}

type StreamOutput struct {
	Outputer
	stream      *tablestream.Stream
	errors      chan error
	inputExists func() *bool
	profile     bool
}

func (o *StreamOutput) CreateStreamFromConfigurationMapping(mapping *conf.Mapping, createNamedQueries []string) error {
	o.stream = &tablestream.Stream{}
	for _, tableDDL := range mapping.Tables {
		err := o.Stream().Query(tableDDL)
		if err != nil {
			return err
		}
	}
	for _, createNamedQuerie := range createNamedQueries {
		found := false
		for _, query := range mapping.Queries {
			if query.Name == createNamedQuerie {
				err := o.Stream().Query(query.Query)
				if err != nil {
					return err
				}
				found = true
			}
		}
		if !found {
			return fmt.Errorf("No query named %s found", createNamedQuerie)
		}
	}

	return nil
}

func (o *StreamOutput) Stream() *tablestream.Stream {
	return o.stream
}

func (o *StreamOutput) ErrorChan() *chan error {
	return &o.errors
}

func (o *StreamOutput) SetInputExists(inputExists func() *bool) {
	o.inputExists = inputExists
}

func (o *StreamOutput) InputExists() *bool {
	if o.Profile() {
		profiling.DumpMemory(fmt.Sprintf("memdump-%d.prof", time.Now().Unix()))
	}
	return o.inputExists()
}

func (o *StreamOutput) EnableProfile() {
	o.profile = true
}

func (o *StreamOutput) Profile() bool {
	return o.profile
}
