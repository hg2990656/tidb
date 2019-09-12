package executor

// scene
var MergeJoinSceneLib []Scene = []Scene{
	originMJScene,
	parallelMJScene,
}

var originMJScene Scene = &MergeJoinScene{
	sceneName: "originMJScene",

	balanceDegree: []float32{0.1, 0.2},
	memUsageRate:  []float32{0.2, 0.3},
	cpuUsageRate:  []float32{0.1, 0.6},
}

var parallelMJScene Scene = &MergeJoinScene{
	sceneName: "parallelMJScene",

	balanceDegree: []float32{0.1, 0.2},
	memUsageRate:  []float32{0.2, 0.3},
	cpuUsageRate:  []float32{0.1, 0.6},
}

// strategy
var MergeJoinStrategyLib []Strategy = []Strategy{
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

//Define mapper relation between scene and strategy.
var MergeJoinMapper map[Scene][]Strategy = map[Scene][]Strategy{
	originMJScene:   {originMJStrategy},
	parallelMJScene: {parallelMJStrategy},
}
