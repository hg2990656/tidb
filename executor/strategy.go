package executor

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"time"
)

//define startegy
type Strategy interface {
	Init(ctx context.Context, e Executor)
	Exec(ctx context.Context, e Executor, req *chunk.Chunk) error
}

type baseStrategy struct {
	strategyName string
}

type OriginMergeJoinStrategy struct {
	baseStrategy

	compareFuncs []expression.CompareFunc //
	isOuterJoin  bool                     //

	prepared bool //
	outerIdx int  //

	innerTable *mergeJoinInnerTable //
	outerTable *mergeJoinOuterTable //

	innerRows     []chunk.Row    //
	innerIter4Row chunk.Iterator //

	//childrenResults []*chunk.Chunk //
}

type ParallelMergeJoinStrategy struct {
	baseStrategy

	compareFuncs []chunk.CompareFunc
	isOuterJoin  bool
	outerIdx     int

	innerTable *parallelMergeJoinInnerTable
	outerTable *parallelMergeJoinOuterTable

	curTask     *mergeTask
	mergeTaskCh <-chan *mergeTask

	//closeCh            chan struct{}
	//joinChkResourceChs []chan *chunk.Chunk
}

func (os *OriginMergeJoinStrategy) Init(ctx context.Context, mergeJoinExec Executor) {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}

	os.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.childrenResults = make([]*chunk.Chunk, 0, len(e.children))
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, newFirstChunk(child))
	}

	os.innerTable.memTracker = memory.NewTracker(innerTableLabel, -1)
	os.innerTable.memTracker.AttachTo(e.memTracker)
}

func (os *OriginMergeJoinStrategy) Exec(ctx context.Context, mergeJoinExec Executor, req *chunk.Chunk) error {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	req.Reset()
	if !os.prepared {
		if err := os.prepare(ctx, e, req.RequiredRows()); err != nil {
			return err
		}
	}

	for !req.IsFull() {
		hasMore, err := os.joinToChunk(ctx, e, req)
		if err != nil || !hasMore {
			return err
		}
	}
	return nil
}

func (ps *ParallelMergeJoinStrategy) Init(ctx context.Context, parallelMergeJoinExec Executor) {
	e, ok := parallelMergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}

	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency

	closeCh := make(chan struct{})
	e.closeCh = closeCh

	taskCh := make(chan *mergeTask, concurrency)
	ps.mergeTaskCh = taskCh

	joinChkResourceChs := make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		joinChkResourceChs[i] = make(chan *chunk.Chunk, 1)
		joinChkResourceChs[i] <- newFirstChunk(e)
	}
	e.joinChkResourceChs = joinChkResourceChs

	//e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	//e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	innerFetchResultCh := make(chan *innerFetchResult, concurrency)
	doneCh := make(chan bool, concurrency)
	closedCh := make(chan bool, concurrency)
	iw := ps.newInnerFetchWorker(innerFetchResultCh, doneCh, closedCh)
	go iw.run(ctx, concurrency)

	mergeJoinWorkerMergeTaskCh := make(chan *mergeTask, concurrency)
	for i := 0; i < concurrency; i++ {
		mw := ps.newMergeJoinWorker(i, innerFetchResultCh, mergeJoinWorkerMergeTaskCh, joinChkResourceChs[i], doneCh, closedCh, e)
		go mw.run(ctx, i)
	}

	ow := ps.newOuterFetchWorker(taskCh, mergeJoinWorkerMergeTaskCh, e)
	go ow.run(ctx)
}

func (ps *ParallelMergeJoinStrategy) Exec(ctx context.Context, mergeJoinExec Executor, req *chunk.Chunk) error {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()

	var err error
	for {
		if ps.curTask == nil {
			ps.curTask = ps.getNextTask(ctx)
			if ps.curTask == nil {
				break
			}

			if ps.curTask.buildErr != nil {
				return ps.curTask.buildErr
			}
		}

		joinResult, ok := <-ps.curTask.joinResultCh
		//curTask process complete, we need getNextTask, so set curTask = nil
		if !ok {
			ps.curTask = nil
			continue
		}

		if joinResult.err != nil {
			err = errors.Trace(joinResult.err)
			break
		}

		req.SwapColumns(joinResult.chk)
		joinResult.src <- joinResult.chk
		break
	}

	return err
}
