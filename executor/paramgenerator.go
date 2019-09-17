package executor

import (
	"fmt"
	"github.com/pingcap/tidb/planner/core"
)

//define params generator
type ParamGenerator interface {
	GetSystemState() *HardWareInfo
	GetStatistic(v core.PhysicalPlan) *StatsInfo
}

//Define our own ParamGenerator HashJoinPG, which implements the interface ParamGenerator.
type MergeJoinPG struct{}

func (mjPG *MergeJoinPG) GetSystemState() *HardWareInfo {
	fmt.Println("get hardware information...")
	return &HardWareInfo{}
}

func (mjPG *MergeJoinPG) GetStatistic(vp core.PhysicalPlan) *StatsInfo {
	fmt.Println("get statistics information...")
	return &StatsInfo{}
}
