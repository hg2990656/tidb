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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"sync"
)

type mtMergeJoinTable struct {
	reader   Executor
	joinKeys []*expression.Column
	ctx      context.Context

	// for chunk executions
	sameKeyRows    []chunk.Row
	compareFuncs   []chunk.CompareFunc
	firstRow4Key   chunk.Row
	curRow         chunk.Row
	curResult      *chunk.Chunk
	curIter        *chunk.Iterator4Chunk
	curResultInUse bool

	memTracker *memory.Tracker
}
type mtMergeJoinOuterTable struct {
	mtMergeJoinTable
	filter []expression.Expression
}

// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mtMergeJoinInnerTable struct {
	mtMergeJoinTable
}

type mtMergejoinWorkerResult struct {
	err error
	chk *chunk.Chunk
	src chan<- *chunk.Chunk
}

type mtMergeJoinCompareWorker struct {
	ctx          sessionctx.Context
	concurrency  int
	compareFuncs []chunk.CompareFunc

	innerRows          []chunk.Row
	innerIter4Row      chunk.Iterator
	innerJoinKeys      []*expression.Column
	innerFetchResultCh <-chan *mtInnerFetchResult

	outerRowIdx        int
	outerSelected      []bool
	outerRow           chunk.Row
	outerRows          []chunk.Row
	outerIter4Row      chunk.Iterator
	outerJoinKeys      []*expression.Column
	outerFetchResultCh <-chan *mtOuterFetchResult

	taskCh            chan<- *mtMergeTask
	mergeWorkerTaskCh chan<- *mtMergeTask
}

type mtMergeJoinMergeWorker struct {
	joiner            joiner
	maxChunkSize      int
	joinChkResourceCh chan *chunk.Chunk

	closeCh     <-chan struct{}
	mergeTaskCh <-chan *mtMergeTask
}

type mtMergeTask struct {
	cmp          int
	buildErr     error
	waitGroup    *sync.WaitGroup
	joinResultCh chan *mtMergejoinWorkerResult

	innerRows []chunk.Row

	outerRows     []chunk.Row
	outerSelected []bool
	outerFrom     int
	outerEnd      int //not included
}

type mtOuterFetchResult struct {
	err        error
	selected   []bool
	fetchRow   []chunk.Row
	memTracker *memory.Tracker
}

type mtInnerFetchResult struct {
	err        error
	fetchRow   []chunk.Row
	memTracker *memory.Tracker
}

type mtMergeJoinInnerFetchWorker struct {
	innerTable    *mtMergeJoinInnerTable
	innerResultCh chan<- *mtInnerFetchResult
}

type mtMergeJoinOuterFetchWorker struct {
	ctx                sessionctx.Context
	outerTable         *mtMergeJoinOuterTable
	outerFetchResultCh chan<- *mtOuterFetchResult
}

func (mw *mtMergeJoinMergeWorker) run(ctx context.Context) {
	ok, joinResult := mw.getNewJoinResult(ctx)
	if !ok {
		return
	}

	var mt *mtMergeTask
	for {
		select {
		case <-ctx.Done():
			ok = false
		case <-mw.closeCh:
			ok = false
		case mt, ok = <-mw.mergeTaskCh:
		}

		if !ok {
			return
		}

		hasMatch := false

		if mt.cmp < 0 {
			var outerRow chunk.Row
			for idx := mt.outerFrom; idx < mt.outerEnd; idx++ {
				outerRow = mt.outerRows[idx]
				mw.joiner.onMissMatch(false, outerRow, joinResult.chk)

				if joinResult.chk.NumRows() == mw.maxChunkSize {
					mt.joinResultCh <- joinResult
					ok, joinResult = mw.getNewJoinResult(ctx)
					if !ok {
						return
					}
				}
			}
		} else {
			innerRows := mt.innerRows
			innerIter4Row := chunk.NewIterator4Slice(innerRows)
			innerIter4Row.Begin()

			var outerRow chunk.Row
			for idx := mt.outerFrom; idx < mt.outerEnd; {
				outerRow = mt.outerRows[idx]
				if !mt.outerSelected[idx] {
					mw.joiner.onMissMatch(false, outerRow, joinResult.chk)
					idx++
				} else {
					matched, _, err := mw.joiner.tryToMatch(outerRow, innerIter4Row, joinResult.chk)
					if err != nil {
						joinResult.err = errors.Trace(err)
						mt.joinResultCh <- joinResult
						return
					}

					hasMatch = hasMatch || matched

					if innerIter4Row.Current() == innerIter4Row.End() {
						if !hasMatch {
							mw.joiner.onMissMatch(false, outerRow, joinResult.chk)
						}
						hasMatch = false
						innerIter4Row.Begin()
						idx++
					}
				}

				if joinResult.chk.NumRows() >= mw.maxChunkSize {
					mt.joinResultCh <- joinResult
					ok, joinResult = mw.getNewJoinResult(ctx)
					if !ok {
						return
					}
				}
			}
		}

		if joinResult.chk.NumRows() > 0 {
			mt.joinResultCh <- joinResult
			ok, joinResult = mw.getNewJoinResult(ctx)
			if !ok {
				return
			}
		}

		mt.waitGroup.Done()
	}
}

func (ow *mtMergeJoinOuterFetchWorker) run(ctx context.Context) { //row with the same key
	defer func() {
		ow.outerTable.memTracker = nil
		close(ow.outerFetchResultCh)
	}()

	fetchResult := &mtOuterFetchResult{}
	err := ow.outerTable.init(ctx, newFirstChunk(ow.outerTable.reader))
	if err != nil {
		fetchResult.err = err
		ow.outerFetchResultCh <- fetchResult
		return
	}

	endRow := ow.outerTable.curIter.End()
	fmt.Println(endRow)

	for {
		fetchResult.fetchRow, fetchResult.err = ow.outerTable.rowsWithSameKey()
		fetchResult.selected, err = expression.VectorizedFilterByRow(ow.ctx, ow.outerTable.filter, fetchResult.fetchRow, fetchResult.selected)

		if len(fetchResult.fetchRow) > 0 || fetchResult.err != nil {
			ow.outerFetchResultCh <- fetchResult
		}

		if err != nil || len(fetchResult.fetchRow) == 0 {
			return
		}
		fetchResult = &mtOuterFetchResult{}
	}
}

func (iw *mtMergeJoinInnerFetchWorker) run(ctx context.Context) {
	defer func() {
		iw.innerTable.memTracker = nil
		close(iw.innerResultCh)
	}()

	fetchResult := &mtInnerFetchResult{}
	err := iw.innerTable.init(ctx, newFirstChunk(iw.innerTable.reader))
	if err != nil {
		fetchResult.err = err
		iw.innerResultCh <- fetchResult
		return
	}

	for {
		fetchResult.fetchRow, fetchResult.err = iw.innerTable.rowsWithSameKey()
		if len(fetchResult.fetchRow) > 0 {
			iw.innerResultCh <- fetchResult
		}
		if err != nil || len(fetchResult.fetchRow) == 0 {
			return
		}
		fetchResult = &mtInnerFetchResult{}
	}
}

func (mw *mtMergeJoinCompareWorker) run(ctx context.Context) {
	defer func() {
		for range mw.innerFetchResultCh {
		}
		close(mw.taskCh)
		close(mw.mergeWorkerTaskCh)
	}()

	if !mw.fetchNextOuterSameKeyGroup(ctx) {
		return
	}

	if !mw.fetchNextInnerSameKeyGroup(ctx) {
		return
	}

	for {
		for mw.outerRow != mw.outerIter4Row.End() && !mw.outerSelected[mw.outerRowIdx] {
			mw.outerRow = mw.outerIter4Row.Next()
			mw.outerRowIdx = mw.outerRowIdx + 1
		}

		cmpResult := -1
		if mw.outerRow != mw.outerIter4Row.End() {
			if len(mw.innerRows) > 0 {
				cmpResult = compareChunkRow(mw.compareFuncs, mw.outerRow, mw.innerRows[0], mw.outerJoinKeys, mw.innerJoinKeys)
			}
		}

		if cmpResult > 0 {
			if !mw.fetchNextInnerSameKeyGroup(ctx) {
				return
			}
			continue
		}

		joinResultCh := make(chan *mtMergejoinWorkerResult)
		waitGroup := new(sync.WaitGroup)
		hasLeft := len(mw.outerRows) % mw.concurrency
		outerRowCountPreTask := len(mw.outerRows) / mw.concurrency
		for idx := 0; idx < len(mw.outerRows); {
			mt := &mtMergeTask{waitGroup: waitGroup}
			mt.cmp = cmpResult
			mt.innerRows = mw.innerRows
			mt.outerRows = mw.outerRows
			mt.outerSelected = mw.outerSelected
			mt.outerFrom = idx
			mt.joinResultCh = joinResultCh
			if len(mw.outerRows) < mw.concurrency {
				mt.outerEnd = idx + 1
				idx = idx + 1
			} else {
				if hasLeft > 0 {
					mt.outerEnd = idx + outerRowCountPreTask + 1
					idx = idx + outerRowCountPreTask + 1
					hasLeft--
				} else {
					mt.outerEnd = idx + outerRowCountPreTask
					idx = idx + outerRowCountPreTask
				}
			}

			waitGroup.Add(1)
			mw.mergeWorkerTaskCh <- mt
			mw.taskCh <- mt
		}

		go func() {
			waitGroup.Wait()
			close(joinResultCh)
		}()

		if !mw.fetchNextOuterSameKeyGroup(ctx) {
			return
		}

		if cmpResult == 0 {
			if !mw.fetchNextInnerSameKeyGroup(ctx) {
				return
			}
		}
	}
}

func (mw *mtMergeJoinCompareWorker) fetchNextInnerSameKeyGroup(ctx context.Context) bool {
	select {
	case innerResult, ok := <-mw.innerFetchResultCh:

		if !ok {
			mw.innerRows = make([]chunk.Row, 0)
			return true
		}

		if innerResult.err != nil {
			mt := &mtMergeTask{buildErr: innerResult.err}
			mw.taskCh <- mt
			return false
		}

		mw.innerRows = innerResult.fetchRow
		mw.innerIter4Row = chunk.NewIterator4Slice(mw.innerRows)
		mw.innerIter4Row.Begin()
		return true
	case <-ctx.Done():
		return false
	}
}

func (mw *mtMergeJoinCompareWorker) fetchNextOuterSameKeyGroup(ctx context.Context) bool {
	select {
	case outerResult, ok := <-mw.outerFetchResultCh:
		if !ok {
			return false
		}
		if outerResult.err != nil {
			mt := &mtMergeTask{buildErr: outerResult.err}
			mw.taskCh <- mt
			return false
		}

		mw.outerRows = outerResult.fetchRow
		mw.outerIter4Row = chunk.NewIterator4Slice(mw.outerRows)
		mw.outerRow = mw.outerIter4Row.Begin()
		mw.outerSelected = outerResult.selected
		mw.outerRowIdx = 0
		return true
	case <-ctx.Done():
		return false
	}
}

func (mw *mtMergeJoinMergeWorker) getNewJoinResult(ctx context.Context) (bool, *mtMergejoinWorkerResult) {
	joinResult := &mtMergejoinWorkerResult{
		src: mw.joinChkResourceCh,
	}
	ok := true
	select {
	case <-ctx.Done():
		ok = false
	case <-mw.closeCh:
		ok = false
	case joinResult.chk, ok = <-mw.joinChkResourceCh:
	}

	return ok, joinResult
}

func (t *mtMergeJoinTable) init(ctx context.Context, chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.ctx = ctx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
	t.compareFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.compareFuncs = append(t.compareFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (t *mtMergeJoinTable) rowsWithSameKey() ([]chunk.Row, error) {
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}
	t.sameKeyRows = make([]chunk.Row, 0)
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextRow()
		endRow := t.curIter.End()
		// error happens or no more data.
		if err != nil || selectedRow == endRow {
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, errors.Trace(err)
		}
		compareResult := compareChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return t.sameKeyRows, nil
		}
	}
}

func (t *mtMergeJoinTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			t.reallocReaderResult()
			oldMemUsage := t.curResult.MemoryUsage()
			//err := t.reader.Next(t.ctx, chunk.NewRecordBatch(t.curResult))
			err := Next(t.ctx, t.reader, t.curResult)
			numRows := t.curResult.NumRows()
			// error happens or no more data.
			if err != nil || numRows == 0 {
				t.curRow = t.curIter.End()
				return t.curRow, errors.Trace(err)
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

func (t *mtMergeJoinTable) hasNullInJoinKey(row chunk.Row) bool {
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
func (t *mtMergeJoinTable) reallocReaderResult() {
	if !t.curResultInUse {
		// If "t.curResult" is not in use, we can just reuse it.
		t.curResult.Reset()
		return
	}

	// NOTE: "t.curResult" is always the last element of "resultQueue".
	t.curResult = newFirstChunk(t.reader)
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curResult.Reset()
	t.curResultInUse = false
}

func (mt *MtMergeJoinStrategy) newOuterFetchWorker(outerFetchResultCh chan<- *mtOuterFetchResult, mergeJoinExec Executor) *mtMergeJoinOuterFetchWorker {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	return &mtMergeJoinOuterFetchWorker{
		outerTable:         mt.outerTable,
		outerFetchResultCh: outerFetchResultCh,
		ctx:                e.ctx,
	}
}

func (mt *MtMergeJoinStrategy) newInnerFetchWorker(innerResultCh chan<- *mtInnerFetchResult) *mtMergeJoinInnerFetchWorker {
	return &mtMergeJoinInnerFetchWorker{
		innerResultCh: innerResultCh,
		innerTable:    mt.innerTable,
	}
}

func (mt *MtMergeJoinStrategy) newCompareWorker(mergeJoinExec Executor, innerFetchResulCh chan *mtInnerFetchResult, outerFetchResultCh chan *mtOuterFetchResult,
	mergeWorkerMergeTaskCh chan *mtMergeTask, taskCh chan *mtMergeTask, concurrency int) *mtMergeJoinCompareWorker {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	return &mtMergeJoinCompareWorker{
		innerFetchResultCh: innerFetchResulCh,
		outerFetchResultCh: outerFetchResultCh,
		ctx:                e.ctx,
		mergeWorkerTaskCh:  mergeWorkerMergeTaskCh,
		taskCh:             taskCh,
		concurrency:        concurrency,
		compareFuncs:       mt.compareFuncs,
		outerJoinKeys:      mt.outerTable.joinKeys,
		innerJoinKeys:      mt.innerTable.joinKeys,
	}
}

func (mt *MtMergeJoinStrategy) newMergeWorker(mergeJoinExec Executor, workerId int, mergeTaskCh chan *mtMergeTask, joinChkResourceCh chan *chunk.Chunk) *mtMergeJoinMergeWorker {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	return &mtMergeJoinMergeWorker{
		closeCh:           e.closeCh,
		mergeTaskCh:       mergeTaskCh,
		joinChkResourceCh: joinChkResourceCh,
		joiner:            e.joiner,
		maxChunkSize:      e.maxChunkSize,
	}
}

func (mt *MtMergeJoinStrategy) getNextTask(ctx context.Context) *mtMergeTask {
	select {
	case task, ok := <-mt.mergeTaskCh:
		if ok {
			return task
		}
	case <-ctx.Done():
		return nil
	}

	return nil
}
