package executor

import (
	"fmt"
)

//define params generator
type ParamGenerator interface {
	GetSystemState() *HardWareInfo
	GetStatistic() *StatsInfo
}

//Define our own ParamGenerator HashJoinPG, which implements the interface ParamGenerator.
type MergeJoinPG struct{}

func (mjPG *MergeJoinPG) GetSystemState() *HardWareInfo {
	fmt.Println("get hardware information...")
	return &HardWareInfo{}
}

func (mjPG *MergeJoinPG) GetStatistic() *StatsInfo {
	fmt.Println("get statistics information...")
	return &StatsInfo{}
}
