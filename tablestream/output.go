package tablestream

import (
	"fmt"
	"io"

	"github.com/olekukonko/tablewriter"
)

func TableWrite(v *View, file io.Writer) {
	data := v.FetchAllRows()
	ptable := tablewriter.NewWriter(file)
	ptable.SetHeader(data[0])
	ptable.AppendBulk(data[1:])
	fmt.Println("\033[2J")
	ptable.Render()
}
