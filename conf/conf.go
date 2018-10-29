package conf

import (
	"io"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Mappings []struct {
		Name    string   `yaml:"name"`
		Tables  []string `yaml:"tables"`
		Queries []struct {
			Name  string `yaml:"name"`
			Query string `yaml:"query"`
		} `yaml:"queries"`
	} `yaml:"mappings"`
	fileName string `yaml,omitempty`
}

func (c *Configuration) ReadFile() error {

	yamlFile, err := ioutil.ReadFile(c.fileName)
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
	f, err := os.Create(c.fileName)
	if err != nil {
		return err
	}

	defer f.Close()
	return c.Write(f)
}

func (c *Configuration) SetFileName(name string) {
	c.fileName = name
}
