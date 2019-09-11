package executor

import "fmt"

type StatsInfo struct {
	nullFrac int
	nDistinct int
	mostCommonVals []interface{}
	mostCommonFreqs []float32
	relTupleNum int64

	//The histogram information of join keys.
	//...
}

type HardWareInfo struct {
	cpuUsageRate float32
	memUsageRate float32
	memCap float32

	//other fields...
	//...
}

//define scene
type Scene interface {
	CompareTo(scene Scene) bool
}

type baseScene struct {
	statsInfo *StatsInfo
	hardwareInfo *HardWareInfo
}

//HashJoinScene implements interface Scene
type MergeJoinScene struct {
	baseScene

	sceneName string

	balanceDegree []float32
	memUsageRate []float32
	cpuUsageRate []float32
}

func (hs *MergeJoinScene) CompareTo(scene Scene) bool {
	fmt.Println("compare our own scene with scene lib...")
	tempHS, ok := scene.(*MergeJoinScene)
	if ok {
		fmt.Println(tempHS)
		return true
	}
	return false
}

