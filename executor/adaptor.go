package executor

import (
	"fmt"
	"github.com/pingcap/tidb/planner/core"
)

//Adaptor is used to acquire strategies dynamically.
type Adaptor interface {
	InitAdaptor(name string)
	Adapt(v core.PhysicalPlan, left Executor, right Executor) Strategy
}

type baseAdaptor struct {
	pg ParamGenerator
	sg SceneGenerator
	mapper *Mapper
	strategy Strategy
	rg *Register
}

//Register is used to provide the init method.
type Register struct {
	registry map[string]func()(ParamGenerator, SceneGenerator)
}

//define our own adaptor to extend baseAdaptor.
type MergeJoinAdapter struct {
	baseAdaptor
}

func (ba *baseAdaptor) initRegister() *Register{
    return &Register{
		registry: make(map[string] func()(ParamGenerator, SceneGenerator)),
	}
}

func (ba *baseAdaptor) addRegister(rg *Register) {
	ba.rg = rg
}

//register the method used for initiating ParamGenerator and SceneGenerator.
func (rg *Register) Register(name string, initGenerator func()(ParamGenerator, SceneGenerator)) {
	rg.registry[name] = initGenerator
}

//Initialization process:
//1.Invokes the register to get the initialization method based on the name you registered
//2.Initiate the ParamGenerator and SceneGenerator
//3.New Mapper and initiate mapper
func (ba *baseAdaptor) InitAdaptor(name string) {
	InitPGAndSG := ba.rg.registry[name]
	ba.pg, ba.sg = InitPGAndSG()
	ba.mapper = &Mapper{}
	ba.mapper.InitMapper()
}

//Startegy getting process:
//1.Invokes ParamGenerator to get statistics and hardware information.
//2.Generate scene according to data characteristics, cpu information and memory information.
//3.According to generated scene to match scene in the scene library.
//4.Use mapper to get startegy what we should use.
func (ba *baseAdaptor) Adapt(vp core.PhysicalPlan, leftExec, rightExec Executor) Strategy{
	fmt.Println("begin to get strategy...")

	hwInfo := ba.pg.GetSystemState()
	statsInfo := ba.pg.GetStatistic()

	// analyze hardware information and statistics information to generate scene
	// different sg(scene generator) has different analysis method
	scene := ba.sg.GenScene(hwInfo, statsInfo)

	matchedScene, ok := ba.mapper.MatchScene(scene)
	if !ok {
		panic("All scenes are matched failed!")
	}

	strategy := ba.mapper.GetStrategy(matchedScene)

	v, ok := vp.(*core.PhysicalMergeJoin)
	if !ok {
		panic("type error")
	}

	leftKeys := v.LeftJoinKeys
	rightKeys := v.RightJoinKeys

	//innerFilter := v.RightConditions

	if ps, ok := strategy.(*originMergeJoinStrategy); ok {
        ps.compareFuncs = v.CompareFuncs
        ps.isOuterJoin = v.JoinType.IsOuterJoin()
        ps.outerIdx = 0
        ps.innerTable = &mergeJoinInnerTable{
			reader:   rightExec,
			joinKeys: rightKeys,
		}
		ps.outerTable = &mergeJoinOuterTable{
			reader: leftExec,
			filter: v.LeftConditions,
			keys:   leftKeys,
		}

		if v.JoinType == core.RightOuterJoin {
			ps.outerIdx = 1
			ps.outerTable.reader = rightExec
			ps.outerTable.filter = v.RightConditions
			ps.outerTable.keys = rightKeys

			//innerFilter = v.LeftConditions
			ps.innerTable.reader = leftExec
			ps.innerTable.joinKeys = leftKeys
		}

		//if len(innerFilter) != 0 {
		//	b.err = errors.Annotate(ErrBuildExecutor, "merge join's inner filter should be empty.")
		//	return nil
		//}
	}

	return strategy
}
