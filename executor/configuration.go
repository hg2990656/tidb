package executor

// scene
var MergeJoinSceneLib []Scene = []Scene{
	parallelMJScene,
}

var parallelMJScene Scene = &MergeJoinScene{
	sceneName:"parallelMJScene",

	balanceDegree: []float32{0.1, 0.2},
	memUsageRate: []float32{0.2, 0.3},
	cpuUsageRate: []float32{0.1, 0.6},
}

// strategy
var MergeJoinStrategyLib []Strategy = []Strategy{
	parallelMJStrategy,
}

var parallelMJStrategy Strategy = &originMergeJoinStrategy{
	baseStrategy: baseStrategy{
		strategyName: "strategyName",
	},
}

//Define mapper relation between scene and strategy.
var MergeJoinMapper map[Scene][]Strategy = map[Scene][]Strategy{
	parallelMJScene:[]Strategy{parallelMJStrategy},
}



