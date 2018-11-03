package conf

import (
	"io"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

type Mapping struct {
	Name    string   `yaml:"name"`
	Tables  []string `yaml:"tables"`
	Queries []struct {
		Name  string `yaml:"name"`
		Query string `yaml:"query"`
	} `yaml:"queries"`
}

type Configuration struct {
	Mappings []Mapping `yaml:"mappings"`
	fileURL  string    `yaml:"fileurl"`
}

func (c *Configuration) ReadFile() error {

	yamlFile, err := ioutil.ReadFile(c.fileURL)
	if err != nil {
		return err
	}

	err = c.parse(yamlFile)
	return err
}

func (c *Configuration) parse(yamlFile []byte) error {
	err := yaml.Unmarshal(yamlFile, c)
	if err != nil {
		return err
	}

	return err
}

func (c *Configuration) Write(file io.Writer) error {
	config, err := yaml.Marshal(c)
	if err != nil {
		return err
	}

	file.Write(config)
	return err
}

func (c *Configuration) WriteFile() error {
	f, err := os.Create(c.fileURL)
	if err != nil {
		return err
	}

	defer f.Close()
	return c.Write(f)
}

func (c *Configuration) SetFileURL(name string) {
	c.fileURL = name
}
