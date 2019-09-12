// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	baseExecutor

	stmtCtx *stmtctx.StatementContext
	//compareFuncs []expression.CompareFunc //
	joiner joiner
	//isOuterJoin  bool //
	//
	//prepared bool //
	//outerIdx int //
	//
	//innerTable *mergeJoinInnerTable //
	//outerTable *mergeJoinOuterTable //
	//
	//innerRows     []chunk.Row //
	//innerIter4Row chunk.Iterator //
	//
	childrenResults []*chunk.Chunk //

	memTracker *memory.Tracker
	adaptor    Adaptor

	closeCh            chan struct{}
	joinChkResourceChs []chan *chunk.Chunk
}

type mergeJoinOuterTable struct {
	reader Executor
	filter []expression.Expression
	keys   []*expression.Column

	chk      *chunk.Chunk
	selected []bool

	iter     *chunk.Iterator4Chunk
	row      chunk.Row
	hasMatch bool
	hasNull  bool
}

// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mergeJoinInnerTable struct {
	reader   Executor
	joinKeys []*expression.Column
	ctx      context.Context

	// for chunk executions
	sameKeyRows    []chunk.Row
	keyCmpFuncs    []chunk.CompareFunc
	firstRow4Key   chunk.Row
	curRow         chunk.Row
	curResult      *chunk.Chunk
	curIter        *chunk.Iterator4Chunk
	curResultInUse bool
	resultQueue    []*chunk.Chunk
	resourceQueue  []*chunk.Chunk

	memTracker *memory.Tracker
}

func (t *mergeJoinInnerTable) init(ctx context.Context, chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.ctx = ctx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	t.resultQueue = append(t.resultQueue, chk4Reader)
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
	t.keyCmpFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.keyCmpFuncs = append(t.keyCmpFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return err
}

func (t *mergeJoinInnerTable) rowsWithSameKey() ([]chunk.Row, error) {
	lastResultIdx := len(t.resultQueue) - 1
	t.resourceQueue = append(t.resourceQueue, t.resultQueue[0:lastResultIdx]...)
	t.resultQueue = t.resultQueue[lastResultIdx:]
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}
	t.sameKeyRows = t.sameKeyRows[:0]
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextRow()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, err
		}
		compareResult := compareChunkRow(t.keyCmpFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return t.sameKeyRows, nil
		}
	}
}

func (t *mergeJoinInnerTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			t.reallocReaderResult()
			oldMemUsage := t.curResult.MemoryUsage()
			err := Next(t.ctx, t.reader, t.curResult)
			// error happens or no more data.
			if err != nil || t.curResult.NumRows() == 0 {
				t.curRow = t.curIter.End()
				return t.curRow, err
			}
			newMemUsage := t.curResult.MemoryUsage()
			t.memTracker.Consume(newMemUsage - oldMemUsage)
			t.curRow = t.curIter.Begin()
		}

		result := t.curRow
		t.curResultInUse = true
		t.curRow = t.curIter.Next()

		if !t.hasNullInJoinKey(result) {
			return result, nil
		}
	}
}

func (t *mergeJoinInnerTable) hasNullInJoinKey(row chunk.Row) bool {
	for _, col := range t.joinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// reallocReaderResult resets "t.curResult" to an empty Chunk to buffer the result of "t.reader".
// It pops a Chunk from "t.resourceQueue" and push it into "t.resultQueue" immediately.
func (t *mergeJoinInnerTable) reallocReaderResult() {
	if !t.curResultInUse {
		// If "t.curResult" is not in use, we can just reuse it.
		t.curResult.Reset()
		return
	}

	// Create a new Chunk and append it to "resourceQueue" if there is no more
	// available chunk in "resourceQueue".
	if len(t.resourceQueue) == 0 {
		newChunk := newFirstChunk(t.reader)
		t.memTracker.Consume(newChunk.MemoryUsage())
		t.resourceQueue = append(t.resourceQueue, newChunk)
	}

	// NOTE: "t.curResult" is always the last element of "resultQueue".
	t.curResult = t.resourceQueue[0]
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.resourceQueue = t.resourceQueue[1:]
	t.resultQueue = append(t.resultQueue, t.curResult)
	t.curResult.Reset()
	t.curResultInUse = false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	e.childrenResults = nil
	e.memTracker = nil

	if e.closeCh != nil {
		close(e.closeCh)
	}

	if len(e.joinChkResourceChs) > 0{
		for _, joinChkResourceCh := range e.joinChkResourceChs {
			close(joinChkResourceCh)
			for range joinChkResourceCh {
			}
		}
	}

	return e.baseExecutor.Close()
}

var innerTableLabel fmt.Stringer = stringutil.StringerStr("innerTable")

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	adapter, ok := e.adaptor.(*MergeJoinAdapter)
	if !ok {
		panic("Adaptor type missmatch!")
	}
	adapter.strategy.Init(ctx, e)

	return nil
}

func compareChunkRow(cmpFuncs []chunk.CompareFunc, lhsRow, rhsRow chunk.Row, lhsKey, rhsKey []*expression.Column) int {
	for i := range lhsKey {
		cmp := cmpFuncs[i](lhsRow, lhsKey[i].Index, rhsRow, rhsKey[i].Index)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (os *OriginMergeJoinStrategy) prepare(ctx context.Context, mergeJoinExec Executor, requiredRows int) error {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	err := os.innerTable.init(ctx, e.childrenResults[os.outerIdx^1])
	if err != nil {
		return err
	}

	err = os.fetchNextInnerRows()
	if err != nil {
		return err
	}

	// init outer table.
	os.outerTable.chk = e.childrenResults[os.outerIdx]
	os.outerTable.iter = chunk.NewIterator4Chunk(os.outerTable.chk)
	os.outerTable.selected = make([]bool, 0, e.maxChunkSize)

	err = os.fetchNextOuterRows(ctx, e, requiredRows)
	if err != nil {
		return err
	}

	os.prepared = true
	return nil
}

// Next implements the Executor Next interface.
func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.Chunk) error {
	adaptor, ok := e.adaptor.(*MergeJoinAdapter)
	if !ok {
		panic("Adaptor type missmatch!")
	}
	err := adaptor.strategy.Exec(ctx, e, req)
	if err != nil {
		return err
	}
	//req.Reset()
	//if !psprepared {
	//	if err := psprepare(ctx, req.RequiredRows()); err != nil {
	//		return err
	//	}
	//}
	//
	//for !req.IsFull() {
	//	hasMore, err := psjoinToChunk(ctx, req)
	//	if err != nil || !hasMore {
	//		return err
	//	}
	//}
	return nil
}

func (os *OriginMergeJoinStrategy) joinToChunk(ctx context.Context, mergeJoinExec Executor, chk *chunk.Chunk) (hasMore bool, err error) {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	for {
		if os.outerTable.row == os.outerTable.iter.End() {
			err = os.fetchNextOuterRows(ctx, e, chk.RequiredRows()-chk.NumRows())
			if err != nil || os.outerTable.chk.NumRows() == 0 {
				return false, err
			}
		}

		cmpResult := -1
		if os.outerTable.selected[os.outerTable.row.Idx()] && len(os.innerRows) > 0 {
			cmpResult, err = os.compare(e, os.outerTable.row, os.innerIter4Row.Current())
			if err != nil {
				return false, err
			}
		}

		if cmpResult > 0 {
			if err = os.fetchNextInnerRows(); err != nil {
				return false, err
			}
			continue
		}

		if cmpResult < 0 {
			e.joiner.onMissMatch(false, os.outerTable.row, chk)
			if err != nil {
				return false, err
			}

			os.outerTable.row = os.outerTable.iter.Next()
			os.outerTable.hasMatch = false
			os.outerTable.hasNull = false

			if chk.IsFull() {
				return true, nil
			}
			continue
		}

		matched, isNull, err := e.joiner.tryToMatch(os.outerTable.row, os.innerIter4Row, chk)
		if err != nil {
			return false, err
		}
		os.outerTable.hasMatch = os.outerTable.hasMatch || matched
		os.outerTable.hasNull = os.outerTable.hasNull || isNull

		if os.innerIter4Row.Current() == os.innerIter4Row.End() {
			if !os.outerTable.hasMatch {
				e.joiner.onMissMatch(os.outerTable.hasNull, os.outerTable.row, chk)
			}
			os.outerTable.row = os.outerTable.iter.Next()
			os.outerTable.hasMatch = false
			os.outerTable.hasNull = false
			os.innerIter4Row.Begin()
		}

		if chk.IsFull() {
			return true, err
		}
	}
}

func (os *OriginMergeJoinStrategy) compare(mergeJoinExec Executor, outerRow, innerRow chunk.Row) (int, error) {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("error")
	}
	outerJoinKeys := os.outerTable.keys
	innerJoinKeys := os.innerTable.joinKeys
	for i := range outerJoinKeys {
		cmp, _, err := os.compareFuncs[i](e.ctx, outerJoinKeys[i], innerJoinKeys[i], outerRow, innerRow)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return int(cmp), nil
		}
	}
	return 0, nil
}

// fetchNextInnerRows fetches the next join group, within which all the rows
// have the same join key, from the inner table.
func (os *OriginMergeJoinStrategy) fetchNextInnerRows() (err error) {
	//e, ok := mergeJoinExec.(*MergeJoinExec)
	//if !ok {
	//	panic("type error")
	//}
	os.innerRows, err = os.innerTable.rowsWithSameKey()
	if err != nil {
		return err
	}
	os.innerIter4Row = chunk.NewIterator4Slice(os.innerRows)
	os.innerIter4Row.Begin()
	return nil
}

// fetchNextOuterRows fetches the next Chunk of outer table. Rows in a Chunk
// may not all belong to the same join key, but are guaranteed to be sorted
// according to the join key.
func (os *OriginMergeJoinStrategy) fetchNextOuterRows(ctx context.Context, mergeJoinExec Executor, requiredRows int) (err error) {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("error")
	}
	// It's hard to calculate selectivity if there is any filter or it's inner join,
	// so we just push the requiredRows down when it's outer join and has no filter.
	if os.isOuterJoin && len(os.outerTable.filter) == 0 {
		os.outerTable.chk.SetRequiredRows(requiredRows, e.maxChunkSize)
	}

	err = Next(ctx, os.outerTable.reader, os.outerTable.chk)
	if err != nil {
		return err
	}

	os.outerTable.iter.Begin()
	os.outerTable.selected, err = expression.VectorizedFilter(e.ctx, os.outerTable.filter, os.outerTable.iter, os.outerTable.selected)
	if err != nil {
		return err
	}
	os.outerTable.row = os.outerTable.iter.Begin()
	return nil
}
