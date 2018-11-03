package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/gfleury/gstreamtop/conf"
	"github.com/gfleury/gstreamtop/input"
	"github.com/gfleury/gstreamtop/output"
)

func main() {
	var mapFile, inputFile string
	var inputFd *os.File
	var err error

	c := &conf.Configuration{}

	var cmdRunNamedQuery = &cobra.Command{
		Use:   "runNamedQuery mappingname queryName",
		Short: "Run a Named SQL query.",
		Long:  `runNamedQuery runs SQL queries against text streams.`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if inputFile == "" {
				inputFd = os.Stdin
			} else {
				inputFd, err = os.Open(inputFile)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}

			c.SetFileURL(mapFile)
			err := c.ReadFile()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			o := &output.SimpleTableOutput{}
			for i, mapping := range c.Mappings {
				if mapping.Name == args[0] {
					err := o.CreateStreamFromConfigurationMapping(&c.Mappings[i], &args[1])
					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
					o.Configure()
					break
				}
				fmt.Printf("No mapping named %s found.\n", args[0])
				os.Exit(1)
			}
			i, err := input.CreateStreamInputFromStreamOutput(o)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			i.Run(inputFd)
			o.Loop()
		},
	}

	var rootCmd = &cobra.Command{Use: "app"}

	rootCmd.PersistentFlags().StringVar(&mapFile, "map", "./mapping.yaml", "config file (default is https://raw.githubusercontent.com/gfleury/gstreamtop/master/mapping.yaml)")
	rootCmd.PersistentFlags().StringVar(&inputFile, "input", "", "input file, default to stdin")

	rootCmd.AddCommand(cmdRunNamedQuery)

	rootCmd.Execute()
}
