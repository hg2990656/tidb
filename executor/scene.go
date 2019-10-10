// add caojun [scene.go] 20191010:b
package executor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
)

type StatsInfo struct {
	nullCounts       []int64
	NDVs             []int64
	mostCommonVals   [][]types.Datum
	mostCommonCounts [][]int64
	relTupleNums     int64

	//The histogram information of join keys.
	//...
}

type HardWareInfo struct {
	cpuUsageRate float64
	memUsageRate float64
	memCap       float64
	availableMem float64

	//other fields...
	//...
}

//define scene
type Scene interface {
	CompareTo(scene Scene) (bool, error)
}

type baseScene struct {
	statsInfo    *StatsInfo
	hardwareInfo *HardWareInfo
}

//MergeJoinScene implements interface Scene
type MergeJoinScene struct {
	baseScene

	sceneName string

	nDVs []int64
	numRows []int64
	memUsageRate []float64
	cpuUsageRate []float64
}

func (ms *MergeJoinScene) CompareTo(scene Scene) (bool, error) {
	//fmt.Println("compare our own scene with scene lib...")
	mjScene, ok := scene.(*MergeJoinScene)
	if !ok {
		return false, errors.Trace(errors.New("Scene's type is not matched."))
	}
	if mjScene.cpuUsageRate[0] >= ms.cpuUsageRate[0] && mjScene.cpuUsageRate[1] < ms.cpuUsageRate[1] {
		if mjScene.memUsageRate[0] >= ms.memUsageRate[0] && mjScene.memUsageRate[1] < ms.memUsageRate[1] {
			if mjScene.numRows[0] >= ms.numRows[0] && mjScene.numRows[1] < ms.numRows[1] {
				flag := true
				for _, nDv := range mjScene.nDVs {
					if nDv < ms.nDVs[0] || nDv >= ms.nDVs[1] {
						flag = false
					}
				}
				if flag {
					return true, nil
				}
			}
		}
	}

	return false, nil
}
// add 20191010:e
