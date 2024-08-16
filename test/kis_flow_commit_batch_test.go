package test

import (
	"context"
	"gopkg.in/yaml.v3"
	"testing"

	"github.com/aceld/kis-flow/file"
	"github.com/aceld/kis-flow/kis"
	"github.com/aceld/kis-flow/log"
)

func TestForkFlowCommitBatch(t *testing.T) {
	ctx := context.Background()

	// 1. Load the configuration file and build the Flow
	if err := file.ConfigImport("load_conf/", func(suffix string) bool {
		if suffix != ".yml" && suffix != ".yaml" {
			return true
		}
		return false
	}, yaml.Unmarshal); err != nil {
		panic(err)
	}

	// 2. Get the Flow
	flow1 := kis.Pool().GetFlow("flowName1")

	stringRows := []string{
		"This is Data1 from Test",
		"This is Data2 from Test",
		"This is Data3 from Test",
	}

	// 3. Commit raw data
	if err := flow1.CommitRowBatch(stringRows); err != nil {
		log.Logger().ErrorF("CommitRowBatch Error, err = %+v", err)
		panic(err)
	}

	// 4. Execute flow1
	if err := flow1.Run(ctx); err != nil {
		panic(err)
	}
}
