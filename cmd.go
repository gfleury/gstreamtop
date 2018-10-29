package main

import (
	"github.com/gfleury/gstreamtop/conf"
	"github.com/gfleury/gstreamtop/tablestream"
)

func main() {
	c := &conf.Configuration{}

	c.SetFileName("mappings.yaml")

	_ = tablestream.CreateTable("salve")
}
