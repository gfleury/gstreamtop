package output

import (
	"github.com/gfleury/gstreamtop/tablestream"
	"os"
	"time"
)

type SimpleTableOutput struct {
	StreamOutput
}

func (o *SimpleTableOutput) Loop() {
	pTicker := time.NewTicker(time.Second * 2)

	for o.inputExists {
		<-pTicker.C
		for _, view := range o.stream.GetViews() {
			tablestream.TableWrite(view, os.Stdout)
		}
	}

}

func (o *SimpleTableOutput) Configure() error {
	o.errors = make(chan error)
	return nil
}

func (o *SimpleTableOutput) Shutdown() {
}
