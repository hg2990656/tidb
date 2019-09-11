package executor

import "fmt"

//define scene generator
type SceneGenerator interface {
	GenScene(hwInfo *HardWareInfo, statsInfo *StatsInfo) Scene
}

//Define our own sceneGenerator HashJoinSG to implements interface SceneGenerator.
type MergeJoinSG struct {}

func (mjSG *MergeJoinSG) GenScene(hwInfo *HardWareInfo, statsInfo *StatsInfo) Scene{
	fmt.Println("analyze hardware information and statistic information and generate our own scene...")
	// analyze the balance of data. calculate variance of mcvFreqs
	mcvFreqs := statsInfo.mostCommonFreqs
	variance := getVariance(mcvFreqs)
	fmt.Println(variance)

	scene := &MergeJoinScene{
		baseScene:baseScene{statsInfo, hwInfo},
		balanceDegree:[]float32{variance, variance},
		cpuUsageRate:[]float32{hwInfo.cpuUsageRate, hwInfo.cpuUsageRate},
		memUsageRate:[]float32{hwInfo.memUsageRate, hwInfo.memUsageRate},
	}
	return scene
}

func getVariance(mvcFreqs []float32) float32 {
	return 0
}

