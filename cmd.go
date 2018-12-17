package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/gfleury/gstreamtop/conf"
	"github.com/gfleury/gstreamtop/input"
	"github.com/gfleury/gstreamtop/output"

	profile "github.com/gfleury/gstreamtop/profiling"
)

func main() {
	var mapFile, inputFile, outputer string
	var inputFd *os.File
	var o output.Outputer
	var err error
	var profiling bool
	var cpuProfileHandler func()

	c := &conf.Configuration{}

	var cmdRunNamedQuery = &cobra.Command{
		Use:   "runNamedQuery mappingname queryName",
		Short: "Run a Named SQL query.",
		Long:  `runNamedQuery runs SQL queries against text streams.`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
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
			}
			if o == nil {
				fmt.Printf("No mapping named %s found.\n", args[0])
				os.Exit(1)
			}
			i, err := input.CreateStreamInputFromStreamOutput(o)
			tables := o.Stream().GetTables()
			i.SetTable(tables[0])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			i.Run(inputFd)
			o.Loop()
		},
	}

	var cmdRunQuery = &cobra.Command{
		Use:   "runQuery mappingname \"SELECT COUNT(*) FROM table GROUP BY field\"",
		Short: "Run a SQL query.",
		Long:  `runQuery runs a SQL queries against text streams.`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			for i, mapping := range c.Mappings {
				if mapping.Name == args[0] {
					err := o.CreateStreamFromConfigurationMapping(&c.Mappings[i], nil)
					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
					err = o.Stream().Query(args[1])
					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
					o.Configure()
					break
				}
			}
			if o == nil {
				fmt.Printf("No mapping named %s found.\n", args[0])
				os.Exit(1)
			}
			i, err := input.CreateStreamInputFromStreamOutput(o)
			tables := o.Stream().GetTables()
			i.SetTable(tables[0])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			i.Run(inputFd)
			o.Loop()
		},
	}

	var rootCmd = &cobra.Command{Use: "app"}

	cobra.OnInitialize(func() {
		switch outputer {
		case "simpletable":
			o = &output.SimpleTableOutput{}
		case "table":
			o = &output.TableOutput{}
		}
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

		if profiling {
			cpuProfileHandler = profile.EnableCPUProfile("gstreamtop.prof")
			o.EnableProfile()
		}
	})

	defer inputFd.Close()
	if cpuProfileHandler != nil {
		defer cpuProfileHandler()
	}

	rootCmd.PersistentFlags().StringVar(&mapFile, "map", "./mapping.yaml", "config file (default is https://raw.githubusercontent.com/gfleury/gstreamtop/master/mapping.yaml)")
	rootCmd.PersistentFlags().StringVar(&inputFile, "input", "", "input file, default to stdin")
	rootCmd.PersistentFlags().StringVar(&outputer, "output", "simpletable", "output method, use: simpletable/table")
	rootCmd.PersistentFlags().BoolVar(&profiling, "profile", false, "enable profiling for CPU and Memory with pprof")

	rootCmd.AddCommand(cmdRunNamedQuery)
	rootCmd.AddCommand(cmdRunQuery)

	rootCmd.Execute()
}
