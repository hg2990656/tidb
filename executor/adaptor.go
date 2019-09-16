package executor

import (
	"fmt"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
)

//Adaptor is used to acquire strategies dynamically.
type Adaptor interface {
	InitAdaptor(name string)
	Adapt(v core.PhysicalPlan, left Executor, right Executor) Strategy
}

type baseAdaptor struct {
	pg       ParamGenerator
	sg       SceneGenerator
	mapper   *Mapper
	strategy Strategy
	rg       *Register
}

//Register is used to provide the init method.
type Register struct {
	registry map[string]func() (ParamGenerator, SceneGenerator)
}

//define our own adaptor to extend baseAdaptor.
type MergeJoinAdapter struct {
	baseAdaptor
}

func (ba *baseAdaptor) initRegister() *Register {
	return &Register{
		registry: make(map[string]func() (ParamGenerator, SceneGenerator)),
	}
}

//register the method used for initiating ParamGenerator and SceneGenerator.
func (rg *Register) Register(name string, initGenerator func() (ParamGenerator, SceneGenerator)) {
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
func (ba *baseAdaptor) Adapt(vp core.PhysicalPlan, leftExec, rightExec Executor) Strategy {
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
	joinType := strategy.GetJoinType()

	switch joinType {
	case 1:
		ba.buildMergeJoinStrategy(vp, leftExec, rightExec, strategy)
		break
	}

	return strategy
}

func (ba *baseAdaptor) buildMergeJoinStrategy(vp core.PhysicalPlan, leftExec, rightExec Executor, strategy Strategy) {
	v, ok := vp.(*core.PhysicalMergeJoin)
	if !ok {
		panic("type error")
	}

	leftKeys := v.LeftJoinKeys
	rightKeys := v.RightJoinKeys

	if os, ok := strategy.(*OriginMergeJoinStrategy); ok {
		os.compareFuncs = v.CompareFuncs
		os.isOuterJoin = v.JoinType.IsOuterJoin()
		os.outerIdx = 0
		os.innerTable = &mergeJoinInnerTable{
			reader:   rightExec,
			joinKeys: rightKeys,
		}
		os.outerTable = &mergeJoinOuterTable{
			reader: leftExec,
			filter: v.LeftConditions,
			keys:   leftKeys,
		}

		if v.JoinType == core.RightOuterJoin {
			os.outerIdx = 1
			os.outerTable.reader = rightExec
			os.outerTable.filter = v.RightConditions
			os.outerTable.keys = rightKeys

			os.innerTable.reader = leftExec
			os.innerTable.joinKeys = leftKeys
		}
	}

	if ps, ok := strategy.(*ParallelMergeJoinStrategy); ok {
		ps.compareFuncs = make([]chunk.CompareFunc, 0, len(v.LeftJoinKeys))
		for i := range v.LeftJoinKeys {
			ps.compareFuncs = append(ps.compareFuncs, chunk.GetCompareFunc(v.LeftJoinKeys[i].RetType))
		}
		ps.outerIdx = 0

		ps.innerTable = &parallelMergeJoinInnerTable{}
		ps.innerTable.reader = rightExec
		ps.innerTable.joinKeys = rightKeys

		ps.outerTable = &parallelMergeJoinOuterTable{}
		ps.outerTable.reader = leftExec
		ps.outerTable.filter = v.LeftConditions
		ps.outerTable.joinKeys = v.LeftJoinKeys

		if v.JoinType == core.RightOuterJoin {
			ps.outerIdx = 1
			ps.outerTable.reader = rightExec
			ps.outerTable.filter = v.RightConditions
			//e.outerTable.keys = rightKeys
			ps.outerTable.joinKeys = rightKeys

			ps.innerTable.reader = leftExec
			ps.innerTable.joinKeys = leftKeys
		}
	}

	if mt, ok := strategy.(*MtMergeJoinStrategy); ok {
		mt.compareFuncs = make([]chunk.CompareFunc, 0, len(v.LeftJoinKeys))
		for i := range v.LeftJoinKeys {
			mt.compareFuncs = append(mt.compareFuncs, chunk.GetCompareFunc(v.LeftJoinKeys[i].RetType))
		}
		mt.outerIdx = 0

		mt.innerTable = &mtMergeJoinInnerTable{}
		mt.innerTable.reader = rightExec
		mt.innerTable.joinKeys = rightKeys

		mt.outerTable = &mtMergeJoinOuterTable{}
		mt.outerTable.reader = leftExec
		mt.outerTable.filter = v.LeftConditions
		mt.outerTable.joinKeys = v.LeftJoinKeys

		if v.JoinType == core.RightOuterJoin {
			mt.outerIdx = 1
			mt.outerTable.reader = rightExec
			mt.outerTable.filter = v.RightConditions
			//e.outerTable.keys = rightKeys
			mt.outerTable.joinKeys = rightKeys

			mt.innerTable.reader = leftExec
			mt.innerTable.joinKeys = leftKeys
		}
	}
}
