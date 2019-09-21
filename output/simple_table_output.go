package output

import (
	"fmt"
	"os"
	"time"

	"github.com/gfleury/gstreamtop/tablestream"
)

type SimpleTableOutput struct {
	StreamOutput
}

func (o *SimpleTableOutput) Loop() {
	pTicker := time.NewTicker(time.Second * 2)

	for *o.InputExists() {
		<-pTicker.C
		fmt.Println("\033[2J")
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
