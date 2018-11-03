package output

import (
	"time"

	ui "github.com/gizak/termui"
	//"github.com/gfleury/gstreamtop/tablestream"
)

type TableOutput struct {
	StreamOutput
	tables []*ui.Table
}

func (o *TableOutput) Loop() {
	pTicker := time.NewTicker(time.Second * 2)
	go func() {
		for {
			for i, view := range o.stream.GetViews() {
				o.tables[i].Rows = view.FetchAllRows()
				ui.Render(o.tables[i])
			}
			<-pTicker.C
		}
	}()

	ui.Handle("q", func(ui.Event) {
		ui.StopLoop()
	})
	ui.Loop()
}

func (o *TableOutput) Configure() error {
	err := ui.Init()
	if err != nil {
		panic(err)
	}

	o.errors = make(chan error)
	views := o.stream.GetViews()
	o.tables = make([]*ui.Table, len(views))
	for i := range o.tables {
		o.tables[i] = ui.NewTable()
		o.tables[i].FgColor = ui.ColorWhite
		o.tables[i].BgColor = ui.ColorDefault
		o.tables[i].Y = 0
		o.tables[i].X = 0
		o.tables[i].Width = 62
		o.tables[i].Height = 7
	}
	return nil
}

func (o *TableOutput) Shutdown() {
	defer ui.Close()
}
