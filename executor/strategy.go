package executor

import (
	"context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

//define startegy
type Strategy interface {
	Init(e Executor)
	Exec(ctx context.Context, e Executor, req *chunk.Chunk) error
}

type baseStrategy struct {
	strategyName string
}

type originMergeJoinStrategy struct {
	baseStrategy

	compareFuncs []expression.CompareFunc //
	isOuterJoin  bool //

	prepared bool //
	outerIdx int //

	innerTable *mergeJoinInnerTable //
	outerTable *mergeJoinOuterTable //

	innerRows     []chunk.Row //
	innerIter4Row chunk.Iterator //

	//childrenResults []*chunk.Chunk //

}

func (os *originMergeJoinStrategy) Init(mergeJoinExec Executor) {
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

func (os *originMergeJoinStrategy) Exec(ctx context.Context, mergeJoinExec Executor, req *chunk.Chunk) error{
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
