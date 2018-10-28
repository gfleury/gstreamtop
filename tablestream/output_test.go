package tablestream

import (
	"bytes"

	"gopkg.in/check.v1"
)

func (s *Suite) TestTableWrite(c *check.C) {
	ibuf := bytes.NewBufferString("")

	obuf := bytes.NewBufferString(`+----------------+----------+----------+
|     SHELL      | COUNT(*) | SUM(GID) |
+----------------+----------+----------+
| /usr/bin/bin   |        3 |      789 |
| /usr/bin/false |        5 |     1315 |
| /usr/bin/true  |        1 |      263 |
+----------------+----------+----------+
`)

	TableWrite(s.stream.views[0], ibuf)

	c.Assert(ibuf.String(), check.Equals, obuf.String())
}
