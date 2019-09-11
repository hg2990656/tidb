package executor

import "fmt"

//define mapper to map scene to strategies
type Mapper struct {
	SceneLib []Scene
	StrategyLib []Strategy
	RelMapper map[Scene] []Strategy
}

//Initiation process:
//1.load scene library and strategy library from configuration file.
//2.load mapper realation between scene and startegy from configuration file.
func (mapper *Mapper) InitMapper() {
	fmt.Println("init mapper...")
	mapper.SceneLib = MergeJoinSceneLib
	mapper.StrategyLib = MergeJoinStrategyLib
	mapper.RelMapper = MergeJoinMapper
}

//match generated scene with scene of scene library
func (mapper *Mapper) MatchScene(scene Scene) (Scene, bool){
	fmt.Println("scene matching...")
	for _, sc := range mapper.SceneLib {
		if sc.CompareTo(scene) {
			return sc, true
		}
	}
	return nil, false
}

//according to matched scene to find matched strategy by mapper.
func (mapper *Mapper) GetStrategy(matchedScene Scene) Strategy {
	return mapper.RelMapper[matchedScene][0]
}
