package executor

// scene
var MergeJoinSceneLib = []Scene{
	parallelMJScene,
	originMJScene,
	mtMJScene,
}

var originMJScene Scene = &MergeJoinScene{
	sceneName: "originMJScene",

	numRows: []int64{0, 100000},
	nDVs: []int64{0, 100000},
	memUsageRate:  []float64{0.4, 0.9},
	cpuUsageRate:  []float64{0, 1},
}

var parallelMJScene Scene = &MergeJoinScene{
	sceneName: "parallelMJScene",

	numRows: []int64{100000, 1000000},
	nDVs: []int64{0, 1000},
	memUsageRate:  []float64{0.4, 0.9},
	cpuUsageRate:  []float64{0, 0.5},
}

var mtMJScene Scene = &MergeJoinScene{
	sceneName: "mtMJScene",

	numRows: []int64{100000, 1000000},
	nDVs: []int64{1000, 1000000},
	memUsageRate:  []float64{0, 0.4},
	cpuUsageRate:  []float64{0, 0.5},
}

// strategy
var MergeJoinStrategyLib = []Strategy{
	mtMJStrategy,
	originMJStrategy,
	parallelMJStrategy,
}

var originMJStrategy Strategy = &OriginMergeJoinStrategy{
	baseStrategy: baseStrategy{
		strategyName: "originStrategy",
	},
}

var parallelMJStrategy Strategy = &ParallelMergeJoinStrategy{
	baseStrategy: baseStrategy{
		strategyName: "parallelStrategy",
	},
}

var mtMJStrategy Strategy = &MtMergeJoinStrategy{
	baseStrategy: baseStrategy{
		strategyName: "mtStrategy",
	},
}

// Define mapper relation between scene and strategy.
var MergeJoinMapper = map[Scene][]Strategy{
	originMJScene:   {originMJStrategy},
	parallelMJScene: {parallelMJStrategy},
	mtMJScene:       {mtMJStrategy},
}
