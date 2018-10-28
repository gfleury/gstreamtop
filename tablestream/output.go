package tablestream

import (
	"io"

	"github.com/olekukonko/tablewriter"
)

func TableWrite(v *View, file io.Writer) {
	data := v.GetAllRows()
	ptable := tablewriter.NewWriter(file)
	ptable.SetHeader(data[0])
	ptable.AppendBulk(data[1:])
	ptable.Render()
}
