package test

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"testing"

	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
)

func TestConfigExportYaml(t *testing.T) {

	// 1. Load the configuration file and build the Flow
	if err := file.ConfigImport("load_conf/", func(suffix string) bool {
		if suffix != ".yml" && suffix != ".yaml" {
			return true
		}
		return false
	}, yaml.Unmarshal); err != nil {
		fmt.Println("Wrong Config Yaml Path!")
		panic(err)
	}

	// 2. Export the built memory KisFlow structure configuration to files
	flows := kis.Pool().GetFlows()
	for _, flow := range flows {
		if err := file.ConfigExport(flow, "/Users/Aceld/go/src/kis-flow/test/export_conf/", yaml.Marshal); err != nil {
			panic(err)
		}
	}
}
