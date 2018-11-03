package input

import (
	"bufio"
	"os"
	"strings"
)

func (i *StreamInput) Configure() error {
	return nil
}

func (i *StreamInput) Loop(file *os.File) {
	var buf string
	reader := bufio.NewReader(file)
	tables := i.stream.GetTables()
	sep := tables[0].RowSeparator()
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
	}
}
