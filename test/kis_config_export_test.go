package test

import (
	"fmt"
	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"testing"
)

func TestConfigExportYmal(t *testing.T) {

	// 1. 加载配置文件并构建Flow
	if err := file.ConfigImportYaml("load_conf/"); err != nil {
		fmt.Println("Wrong Config Yaml Path!")
		panic(err)
	}

	// 2. 讲构建的内存KisFlow结构配置导出的文件当中
	flows := kis.Pool().GetFlows()
	for _, flow := range flows {
		if err := file.ConfigExportYaml(flow, "/Users/Aceld/go/src/kis-flow/test/export_conf/"); err != nil {
			panic(err)
		}
	}
}
