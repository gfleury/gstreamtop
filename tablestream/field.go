package tablestream

import (
	"time"

	"github.com/araddon/dateparse"
)

type fieldType string

const (
	VARCHAR  fieldType = "varchar"
	INTEGER  fieldType = "integer"
	DATETIME fieldType = "datetime"
)

type Field struct {
	name      string
	fieldType fieldType
}

func parseDate(datetime string) (dt time.Time, err error) {
	dt, err = dateparse.ParseAny(datetime)
	if err != nil {
		//	Mon Jan 2 15:04:05 -0700 MST 2006
		// 20/May/2015:21:05:35 +0000
		dt, err = time.Parse("02/Jan/2006:15:04:05 -0700", datetime)
	}
	return dt, err
}
