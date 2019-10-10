// add caojun [scenegenerator.go] 20191010:b
package executor

//define scene generator
type SceneGenerator interface {
	GenScene(hwInfo *HardWareInfo, statsInfo *StatsInfo) Scene
}

//Define our own sceneGenerator HashJoinSG to implements interface SceneGenerator.
type MergeJoinSG struct {

}

func (mjSG *MergeJoinSG) GenScene(hwInfo *HardWareInfo, statsInfo *StatsInfo) Scene {
	//fmt.Println("analyze hardware information and statistic information and generate our own scene...")
	// analyze the balance of data. calculate variance of mcvFreqs
	// statsInfo.NDVs contain all join key columns' distinct value
	var nDVs []int64
	for _, num := range statsInfo.NDVs {
		nDVs = append(nDVs, num)
	}

	scene := &MergeJoinScene{
		baseScene:     baseScene{statsInfo, hwInfo},
		cpuUsageRate:  []float64{hwInfo.cpuUsageRate, hwInfo.cpuUsageRate},
		memUsageRate:  []float64{hwInfo.memUsageRate, hwInfo.memUsageRate},
		numRows: []int64{statsInfo.relTupleNums, statsInfo.relTupleNums},
		nDVs: nDVs,
	}
	return scene
}

func getVariance(mcvCount []int64) float64 {
	var quadraticSum float64
	var sum float64
	var count int64
	for i := range mcvCount {
		quadraticSum += float64(mcvCount[i] * mcvCount[i])
		sum += float64(mcvCount[i])
		count++
	}
	result := quadraticSum/float64(count) - (sum*sum)/float64(count*count)
	return result
}
// add 20191010:e
