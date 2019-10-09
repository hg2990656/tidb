package executor

import (
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
)

//Adaptor is used to acquire strategies dynamically.
type Adaptor interface {
	InitAdaptor(name string)
	Adapt() (Strategy, error)
	GetStrategy() Strategy
}

type BaseAdaptor struct {
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
	BaseAdaptor
}

func NewRegister() *Register {
	rg := &Register{
		registry: make(map[string]func() (ParamGenerator, SceneGenerator)),
	}
	return rg
}

//register the method used for initiating ParamGenerator and SceneGenerator.
func (rg *Register) Register(name string, initGenerator func() (ParamGenerator, SceneGenerator)) {
	rg.registry[name] = initGenerator
}

//Initialization process:
//1.Invokes the register to get the initialization method based on the name you registered
//2.Initiate the ParamGenerator and SceneGenerator
//3.New Mapper and initiate mapper
func (ba *BaseAdaptor) InitAdaptor(name string) {
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
func (ba *BaseAdaptor) Adapt() (Strategy, error) {
	hwInfo, err := ba.pg.GetSystemState()
	if err != nil {
		return nil, err
	}
	statsInfo, err := ba.pg.GetStatistic()
	if err != nil {
		return nil, err
	}

	// analyze hardware information and statistics information to generate scene
	// different sg(scene generator) has different analysis method
	scene := ba.sg.GenScene(hwInfo, statsInfo)
	matchedScene, err := ba.mapper.MatchScene(scene)
	if err != nil {
		return nil, err
	} else if matchedScene == nil {
		// use the first default strategy
		return ba.mapper.StrategyLib[0], nil
	}

	strategy := ba.mapper.GetStrategy(matchedScene)
	return strategy, nil
}

func (ba *BaseAdaptor) GetStrategy() Strategy {
	return ba.strategy
}

func (ba *BaseAdaptor) BindingToAdaptor(rg *Register) {
	ba.rg = rg
}

func (ba *BaseAdaptor) SetStrategy(sg Strategy) {
	ba.strategy = sg
}

func (os *OriginMergeJoinStrategy) buildOriginMergeJoinStrategy(mergeJoinExec Executor) {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}

	os.compareFuncs = e.compareFuncs
	os.isOuterJoin = e.isOuterJoin
	os.outerIdx = 0
	os.innerTable = &mergeJoinInnerTable{
		reader:   e.rightExec,
		joinKeys: e.rightKeys,
	}
	os.outerTable = &mergeJoinOuterTable{
		reader: e.leftExec,
		filter: e.leftConditions,
		keys:   e.leftKeys,
	}

	if e.joinType == core.RightOuterJoin {
		os.outerIdx = 1
		os.outerTable.reader = e.rightExec
		os.outerTable.filter = e.rightConditions
		os.outerTable.keys = e.rightKeys

		os.innerTable.reader = e.leftExec
		os.innerTable.joinKeys = e.leftKeys
	}
}

func (ps *ParallelMergeJoinStrategy) buildParallelMergeJoinStrategy(mergeJoinExec Executor) {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}

	ps.compareFuncs = make([]chunk.CompareFunc, 0, len(e.leftKeys))
	for i := range e.leftKeys {
		ps.compareFuncs = append(ps.compareFuncs, chunk.GetCompareFunc(e.leftKeys[i].RetType))
	}
	ps.outerIdx = 0

	ps.innerTable = &parallelMergeJoinInnerTable{}
	ps.innerTable.reader = e.rightExec
	ps.innerTable.joinKeys = e.rightKeys

	ps.outerTable = &parallelMergeJoinOuterTable{}
	ps.outerTable.reader = e.leftExec
	ps.outerTable.filter = e.leftConditions
	ps.outerTable.joinKeys = e.leftKeys

	if e.joinType == core.RightOuterJoin {
		ps.outerIdx = 1
		ps.outerTable.reader = e.rightExec
		ps.outerTable.filter = e.rightConditions
		//e.outerTable.keys = rightKeys
		ps.outerTable.joinKeys = e.rightKeys

		ps.innerTable.reader = e.leftExec
		ps.innerTable.joinKeys = e.leftKeys
	}
}

func (mt *MtMergeJoinStrategy) buildMtParallelMergeJoinStrategy(mergeJoinExec Executor){
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}

	mt.compareFuncs = make([]chunk.CompareFunc, 0, len(e.leftKeys))
	for i := range e.leftKeys {
		mt.compareFuncs = append(mt.compareFuncs, chunk.GetCompareFunc(e.leftKeys[i].RetType))
	}
	mt.outerIdx = 0

	mt.innerTable = &mtMergeJoinInnerTable{}
	mt.innerTable.reader = e.rightExec
	mt.innerTable.joinKeys = e.rightKeys

	mt.outerTable = &mtMergeJoinOuterTable{}
	mt.outerTable.reader = e.leftExec
	mt.outerTable.filter = e.leftConditions
	mt.outerTable.joinKeys = e.leftKeys

	if e.joinType == core.RightOuterJoin {
		mt.outerIdx = 1
		mt.outerTable.reader = e.rightExec
		mt.outerTable.filter = e.rightConditions
		//e.outerTable.keys = rightKeys
		mt.outerTable.joinKeys = e.rightKeys

		mt.innerTable.reader = e.leftExec
		mt.innerTable.joinKeys = e.leftKeys
	}
}

//func (ba *BaseAdaptor) buildMergeJoinStrategy(vp core.PhysicalPlan, leftExec, rightExec Executor, strategy Strategy) {
//	v, ok := vp.(*core.PhysicalMergeJoin)
//	if !ok {
//		panic("type error")
//	}
//
//	leftKeys := v.LeftJoinKeys
//	rightKeys := v.RightJoinKeys
//
//	if os, ok := strategy.(*OriginMergeJoinStrategy); ok {
//		os.compareFuncs = v.CompareFuncs
//		os.isOuterJoin = v.JoinType.IsOuterJoin()
//		os.outerIdx = 0
//		os.innerTable = &mergeJoinInnerTable{
//			reader:   rightExec,
//			joinKeys: rightKeys,
//		}
//		os.outerTable = &mergeJoinOuterTable{
//			reader: leftExec,
//			filter: v.LeftConditions,
//			keys:   leftKeys,
//		}
//
//		if v.JoinType == core.RightOuterJoin {
//			os.outerIdx = 1
//			os.outerTable.reader = rightExec
//			os.outerTable.filter = v.RightConditions
//			os.outerTable.keys = rightKeys
//
//			os.innerTable.reader = leftExec
//			os.innerTable.joinKeys = leftKeys
//		}
//	}
//
//	if ps, ok := strategy.(*ParallelMergeJoinStrategy); ok {
//		ps.compareFuncs = make([]chunk.CompareFunc, 0, len(v.LeftJoinKeys))
//		for i := range v.LeftJoinKeys {
//			ps.compareFuncs = append(ps.compareFuncs, chunk.GetCompareFunc(v.LeftJoinKeys[i].RetType))
//		}
//		ps.outerIdx = 0
//
//		ps.innerTable = &parallelMergeJoinInnerTable{}
//		ps.innerTable.reader = rightExec
//		ps.innerTable.joinKeys = rightKeys
//
//		ps.outerTable = &parallelMergeJoinOuterTable{}
//		ps.outerTable.reader = leftExec
//		ps.outerTable.filter = v.LeftConditions
//		ps.outerTable.joinKeys = v.LeftJoinKeys
//
//		if v.JoinType == core.RightOuterJoin {
//			ps.outerIdx = 1
//			ps.outerTable.reader = rightExec
//			ps.outerTable.filter = v.RightConditions
//			//e.outerTable.keys = rightKeys
//			ps.outerTable.joinKeys = rightKeys
//
//			ps.innerTable.reader = leftExec
//			ps.innerTable.joinKeys = leftKeys
//		}
//	}
//
//	if mt, ok := strategy.(*MtMergeJoinStrategy); ok {
//		mt.compareFuncs = make([]chunk.CompareFunc, 0, len(v.LeftJoinKeys))
//		for i := range v.LeftJoinKeys {
//			mt.compareFuncs = append(mt.compareFuncs, chunk.GetCompareFunc(v.LeftJoinKeys[i].RetType))
//		}
//		mt.outerIdx = 0
//
//		mt.innerTable = &mtMergeJoinInnerTable{}
//		mt.innerTable.reader = rightExec
//		mt.innerTable.joinKeys = rightKeys
//
//		mt.outerTable = &mtMergeJoinOuterTable{}
//		mt.outerTable.reader = leftExec
//		mt.outerTable.filter = v.LeftConditions
//		mt.outerTable.joinKeys = v.LeftJoinKeys
//
//		if v.JoinType == core.RightOuterJoin {
//			mt.outerIdx = 1
//			mt.outerTable.reader = rightExec
//			mt.outerTable.filter = v.RightConditions
//			//e.outerTable.keys = rightKeys
//			mt.outerTable.joinKeys = rightKeys
//
//			mt.innerTable.reader = leftExec
//			mt.innerTable.joinKeys = leftKeys
//		}
//	}
//}
