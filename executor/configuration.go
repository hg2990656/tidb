package executor

// scene
var MergeJoinSceneLib = []Scene{
	mtMJScene,
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

var mtMJScene Scene = &MergeJoinScene{
	sceneName: "mtMJScene",

	balanceDegree: []float32{0.1, 0.2},
	memUsageRate:  []float32{0.2, 0.3},
	cpuUsageRate:  []float32{0.1, 0.6},
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
		joinType: 1,
	},
}

var parallelMJStrategy Strategy = &ParallelMergeJoinStrategy{
	baseStrategy: baseStrategy{
		strategyName: "parallelStrategy",
		joinType: 1,
	},
}

var mtMJStrategy Strategy = &MtMergeJoinStrategy{
	baseStrategy: baseStrategy{
		strategyName: "mtStrategy",
		joinType: 1,
	},
}

// Define mapper relation between scene and strategy.
var MergeJoinMapper = map[Scene][]Strategy{
	originMJScene:   {originMJStrategy},
	parallelMJScene: {parallelMJStrategy},
	mtMJScene:       {mtMJStrategy},
}
