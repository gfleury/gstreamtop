package output

import (
	"fmt"
	"github.com/gfleury/gstreamtop/conf"
	"github.com/gfleury/gstreamtop/tablestream"
)

type Outputer interface {
	Loop()
	Configure() error
	Stream() *tablestream.Stream
	ErrorChan() *chan error
}

type StreamOutput struct {
	Outputer
	stream *tablestream.Stream
	errors chan error
}

func (o *StreamOutput) CreateStreamFromConfigurationMapping(mapping *conf.Mapping, createNamedQueries *string) error {
	o.stream = &tablestream.Stream{}
	for _, tableDDL := range mapping.Tables {
		err := o.stream.Query(tableDDL)
		if err != nil {
			return err
		}
	}
	if createNamedQueries != nil {
		for _, query := range mapping.Queries {
			if query.Name == *createNamedQueries {
				err := o.stream.Query(query.Query)
				return err
			}
		}
		return fmt.Errorf("No query named %s found", *createNamedQueries)
	}
	return nil
}

func (o *StreamOutput) Stream() *tablestream.Stream {
	return o.stream
}

func (o *StreamOutput) ErrorChan() *chan error {
	return &o.errors
}
