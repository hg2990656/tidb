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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"golang.org/x/net/context"
)

type mergeJoinTable struct {
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

type parallelMergeJoinOuterTable struct {
	mergeJoinTable
	filter   []expression.Expression
	selected []bool
}

type parallelMergeJoinInnerTable struct {
	mergeJoinTable
}

type mergeJoinWorkerResult struct {
	err error
	chk *chunk.Chunk
	src chan<- *chunk.Chunk
}

type mergeJoinWorker struct {
	joiner            joiner
	maxChunkSize      int
	joinChkResourceCh chan *chunk.Chunk

	closeCh     <-chan struct{}
	mergeTaskCh <-chan *mergeTask

	innerFetchResultCh <-chan *innerFetchResult

	innerChunk    *chunk.Chunk
	innerIter4Row *chunk.Iterator4Chunk
	curInnerRow   chunk.Row
	firstRow4Key  chunk.Row
	sameKeyRows   []chunk.Row
	compareFuncs  []chunk.CompareFunc

	innerCache []chunk.Row

	outerJoinKeys []*expression.Column
	innerJoinKeys []*expression.Column

	doneCh          chan<- bool
	closedCh        chan<- bool
	rowsWithSameKey []chunk.Row
}

type mergeTask struct {
	buildErr     error
	joinResultCh chan *mergeJoinWorkerResult

	outerRows     []chunk.Row
	outerSelected []bool

	lastTaskFlag  bool
	firstTaskFlag bool

	number int
}

type innerFetchResult struct {
	err        error
	fetchChunk *chunk.Chunk
	memTracker *memory.Tracker
}

type outerFetchWorker struct {
	ctx        sessionctx.Context
	outerTable *parallelMergeJoinOuterTable

	resultCh         chan<- *mergeTask // send to main thread
	joinWorkerTaskCh chan<- *mergeTask // send to join worker
	maxChunkSize     int
}

type innerFetchWorker struct {
	innerTable    *parallelMergeJoinInnerTable
	innerResultCh chan<- *innerFetchResult
	doneCh        <-chan bool
	closedCh      <-chan bool
}

func (ps *ParallelMergeJoinStrategy) newInnerFetchWorker(innerResultCh chan<- *innerFetchResult, doneCh, closedCh chan bool) *innerFetchWorker {
	return &innerFetchWorker{
		innerResultCh: innerResultCh,
		innerTable:    ps.innerTable,
		doneCh:        doneCh,
		closedCh:      closedCh,
	}
}

func (ps *ParallelMergeJoinStrategy) newOuterFetchWorker(taskCh, mergeJoinWorkerMergeTaskCh chan *mergeTask, mergeJoinExec Executor) *outerFetchWorker {
    e, ok := mergeJoinExec.(*MergeJoinExec)
    if !ok {
    	panic("type error")
	}
	return &outerFetchWorker{
		outerTable:       ps.outerTable,
		ctx:              e.ctx,
		resultCh:         taskCh,
		joinWorkerTaskCh: mergeJoinWorkerMergeTaskCh,
		maxChunkSize:     e.maxChunkSize,
	}
}

func (ps *ParallelMergeJoinStrategy) newMergeJoinWorker(workerId int, innerFetchResultCh chan *innerFetchResult, mergeJoinWorkerMergeTaskCh chan *mergeTask, joinChkResourceCh chan *chunk.Chunk, doneCh, closedCh chan<- bool, mergeJoinExec Executor) *mergeJoinWorker {
	e, ok := mergeJoinExec.(*MergeJoinExec)
	if !ok {
		panic("type error")
	}
	return &mergeJoinWorker{
		closeCh:            e.closeCh,
		innerFetchResultCh: innerFetchResultCh,
		mergeTaskCh:        mergeJoinWorkerMergeTaskCh,
		joinChkResourceCh:  joinChkResourceCh,
		joiner:             e.joiner,
		maxChunkSize:       e.maxChunkSize,
		outerJoinKeys:      ps.outerTable.joinKeys,
		innerJoinKeys:      ps.innerTable.joinKeys,
		compareFuncs:       ps.compareFuncs,
		doneCh:             doneCh,
		closedCh:           closedCh,
	}
}

// 1.outer fetch worker get the outer chunk from outer table
// 2.range the outer chunk and generate several merge task which contain the same join key rows
// 3.send the merge task to join worker and main thread
func (ow *outerFetchWorker) run(ctx context.Context) {
	defer func() {
		close(ow.resultCh)
		close(ow.joinWorkerTaskCh)
	}()

	// init outer table and get the first outer chunk
	err := ow.outerTable.init(ctx, newFirstChunk(ow.outerTable.reader))
	if err != nil {
		return
	}

	count := 0

	for {
		if ow.outerTable.curRow == ow.outerTable.curIter.End() && ow.outerTable.firstRow4Key == ow.outerTable.curIter.End() {
			err := ow.fetchNextOuterChunk(ctx)
			if err != nil || ow.outerTable.curResult.NumRows() == 0 {
				break
			}
		}
		// generate merge task, every task contain same join key rows in this chunk as outerRows
		mt := &mergeTask{}
		outerRows, err := ow.outerTable.outerRowsWithSameKey()
		if err != nil {
			break
		}
		count++
		mt.outerRows = outerRows
		mt.number = count

		// joinResultCh is the channel between the join worker and main thread
		// join worker get the merge task and send the join result to task's joinResultCh, then main thread get the join result
		joinResultCh := make(chan *mergeJoinWorkerResult)
		mt.joinResultCh = joinResultCh

		if ow.outerTable.curRow == ow.outerTable.curIter.End() {
			mt.lastTaskFlag = true
		}

		// send the merge task to join worker
		if finished := ow.pushToChan(ctx, mt, ow.joinWorkerTaskCh); finished {
			return
		}

		// send the merge task to main thread
		if finished := ow.pushToChan(ctx, mt, ow.resultCh); finished {
			return
		}
	}
}

func (t *mergeJoinTable) outerRowsWithSameKey() ([]chunk.Row, error) {
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}

	var sameKeyRows []chunk.Row
	sameKeyRows = append(sameKeyRows, t.firstRow4Key)

	for {
		selectedRow, err := t.nextRow()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			return sameKeyRows, err
		}
		compareResult := compareIOChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			sameKeyRows = append(sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return sameKeyRows, nil
		}
	}
}

func (t *mergeJoinTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			return t.curRow, nil
		}
		result := t.curRow
		t.curResultInUse = true
		t.curRow = t.curIter.Next()

		if !t.hasNullInJoinKey(result) {
			return result, nil
		}
	}
}

func (ow *outerFetchWorker) pushToChan(ctx context.Context, task *mergeTask, dst chan<- *mergeTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

func (ow *outerFetchWorker) fetchNextOuterChunk(ctx context.Context) (err error) {
	ow.outerTable.reAllocReaderResult()

	err = Next(ctx, ow.outerTable.reader, ow.outerTable.curResult)
	if err != nil {
		return err
	}

	ow.outerTable.curIter.Begin()
	ow.outerTable.selected, err = expression.VectorizedFilter(ow.ctx, ow.outerTable.filter, ow.outerTable.curIter, ow.outerTable.selected)
	if err != nil {
		return err
	}
	//ow.outerTable.curRow = ow.outerTable.curIter.Begin()
	ow.outerTable.firstRow4Key = ow.outerTable.curIter.Begin()
	ow.outerTable.curRow = ow.outerTable.curIter.Next()
	return nil
}

// 1.inner fetch worker get the inner chunk from inner table and send to every join worker
// 2.only all join worker send doneCh to inner worker, inner worker can get and send the next inner chunk to all join worker
func (iw *innerFetchWorker) run(ctx context.Context, concurrency int) {
	defer func() {
		close(iw.innerResultCh)
	}()

	fetchResult := &innerFetchResult{}
	err := iw.innerTable.init(ctx, newFirstChunk(iw.innerTable.reader))
	if err != nil {
		fetchResult.err = err
		iw.innerResultCh <- fetchResult
		return
	}

	// send the first inner chunk to all join worker
	fetchResult.fetchChunk = iw.innerTable.curResult
	fetchResult.err = err
	for i := 0; i < concurrency; i++ {
		iw.innerResultCh <- fetchResult
	}

	var count int
	var ok, done, closed bool
	alive := concurrency

	for {
		select {
		case <-ctx.Done():
			ok = false
		case done, ok = <-iw.doneCh:
			if done {
				count += 1
			}
		case closed, ok = <-iw.closedCh:
			if closed {
				alive = alive - 1
			}
		}

		if !ok {
			return
		}

		if count == alive {
			fetchResult = &innerFetchResult{}
			// a new chunk is needed, otherwise the columns in joinWorker's outerRowsWithSameKey will be cleared
			iw.innerTable.curResult = newFirstChunk(iw.innerTable.reader)

			err = Next(ctx, iw.innerTable.reader, iw.innerTable.curResult)
			if err != nil || iw.innerTable.curResult.NumRows() == 0 {
				return
			}

			fetchResult.fetchChunk = iw.innerTable.curResult
			fetchResult.err = err
			for i := 0; i < alive; i++ {
				iw.innerResultCh <- fetchResult
			}

			count = 0
		}
	}
}

// 1.merge join worker get the first inner chunk from inner fetch worker and get the first innerRowsWithSameKey
// 2.get the merge task from outer fetch worker
// 3.compare merge task's outer rows with innerRowsWithSameKey and try to join
// 4.send the join result by the merge task's joinResultCh and close this task's joinResultCh
// 5.get the next merge task from outer fetch worker, if mergeTaskCh is closed, then close this goroutine
func (jw *mergeJoinWorker) run(ctx context.Context, i int) {
	ok, joinResult := jw.getNewJoinResult(ctx)
	if !ok {
		return
	}

	// 1.merge join worker get the first inner chunk from inner fetch worker and get the first innerRowsWithSameKey
	if !jw.fetchNextInnerChunk(ctx) {
		return
	}
	rowsWithSameKey, err := jw.innerRowsWithSameKey()
	if err != nil {
		return
	}

	// 2.get merge task from outerFetchWorker
	//var s int
	var mt *mergeTask

	for {
		select {
		case <-ctx.Done():
			ok = false
			//s = 1
		case <-jw.closeCh:
			ok = false
			//s = 2
		case mt, ok = <-jw.mergeTaskCh:
			//s = 3
		}

		if !ok {
			jw.closedCh <- true
			//fmt.Println("Number ", i, " goroutine close for:", s)
			return
		}

		//fmt.Println("goroutine", i, "deals with Merge Task number:", mt.number)

		// whether get the next inner chunk
		trap := false

		// 3.get the sameKeyGroup inner rows from inner chunk and compare with outer table rows in merge task
		for {
			cmpResult := -1

			if len(rowsWithSameKey) > 0 {
				cmpResult = compareIOChunkRow(jw.compareFuncs, mt.outerRows[0], rowsWithSameKey[0], jw.outerJoinKeys, jw.innerJoinKeys)
			}

			// if cmpResult > 0, outerRows's join key value is greater than innerRowsWithSameKey's join key value
			// then get the next innerRowsWithSameKey from inner chunk
			if cmpResult > 0 {
				rowsWithSameKey, err = jw.innerRowsWithSameKey()
				if err != nil {
					return
				}
				if len(rowsWithSameKey) == 0 {
					// (1) send doneCh to inner worker
					jw.doneCh <- true
					// (2) then get the next inner chunk
					if !jw.fetchNextInnerChunk(ctx) {
						//mt.joinResultCh <- joinResult
						close(mt.joinResultCh)
						break
					}
					// tested trap: 2019-09-02 10:45
					trap = true
					rowsWithSameKey, err = jw.innerRowsWithSameKey()
					if err != nil {
						return
					}
				}
				continue
			}

			// if cmpResult < 0, consider two situations:
			// 1.do not get the next inner chunk, then do on miss match (tested)
			// 2.has got the next inner chunk, but now outerRowsWithSameKey's join key value is equal to last inner chunk's last innerRowsWithSameKey
			// and is smaller than this new inner chunk's first innerRowsWithSameKey
			if cmpResult < 0 {
				// tested trap  2019-9-2 10:03
				if trap && len(jw.innerCache) > 0 {
					cmpCache := -1
					cmpCache = compareIOChunkRow(jw.compareFuncs, mt.outerRows[0], jw.innerCache[0], jw.outerJoinKeys, jw.innerJoinKeys)
					// now outerRowsWithSameKey's join key value is equal to last inner chunk's last innerRowsWithSameKey, do join with innerCache
					if cmpCache == 0 {
						innerIter4Row := chunk.NewIterator4Slice(jw.innerCache)
						innerIter4Row.Begin()

						ok = jw.tryToJoin(ctx, mt, innerIter4Row, joinResult)
						jw.innerCache = jw.innerCache[:0]
						trap = false
						if !ok {
							return
						}
					}
				} else {
					// do on miss match
					for _, outerRow := range mt.outerRows {
						jw.joiner.onMissMatch(false, outerRow, joinResult.chk)

						if joinResult.chk.NumRows() == jw.maxChunkSize {
							mt.joinResultCh <- joinResult
							ok, joinResult = jw.getNewJoinResult(ctx)
							if !ok {
								return
							}
						}
					}
				}
			} else {
				// if cmpResult == 0, consider four situations:
				// 1.merge task is the last merge task in the outer chunk and innerRowsWithSameKey is also the last batch in inner chunk
				// 2.merge task is the last merge task in the outer chunk but innerRowsWithSameKey is not the last batch in inner chunk
				// 3.merge task is not the last merge task but innerRowsWithSameKey is the last batch in inner chunk
				// 4.merge task is not the last merge task and innerRowsWithSameKey is also not the last batch in inner chunk
				// in situation 1, 2, 3, it's needed to put the innerRowsWithSameKey into jw.innerCache
				if mt.lastTaskFlag || jw.curInnerRow == jw.innerIter4Row.End() {
					// note: when jw.curInnerRow == jw.innerIter4Row.End(), jw.firstRow4Key may not be equal to jw.innerIter4Row.End()
					// if jw.firstRow4Key is the last row in now inner chunk, so there is still last innerRowsWithSameKey which just contain the last row
					if jw.curInnerRow == jw.innerIter4Row.End() && jw.firstRow4Key == jw.innerIter4Row.End() {
						// here are two situations:
						// (1) merge task is not the last one in outer chunk and there is no more innerRowsWithSameKey in inner chunk - 3
						// (2) merge task is the last one in outer chunk and there is no more innerRowsWithSameKey in inner chunk - 1
						jw.doneCh <- true
						if !jw.fetchNextInnerChunk(ctx) {
							var innerIter4Row chunk.Iterator
							// trap = true, innerRowsWithSameKey contain all the rows of the second inner chunk and it's join key
							// is equal to the last innerRowsWithSameKey in the first inner chunk
							if trap && len(jw.innerCache) > 0 {
								for _, row := range rowsWithSameKey {
									jw.innerCache = append(jw.innerCache, row)
								}
								//jw.innerCache = append(jw.innerCache, rowsWithSameKey...)
								innerIter4Row = chunk.NewIterator4Slice(jw.innerCache)
								innerIter4Row.Begin()
							} else {
								// no next inner chunk and no jw.innerCache, so just do join with now innerRowsWithSameKey
								innerIter4Row = chunk.NewIterator4Slice(rowsWithSameKey)
								innerIter4Row.Begin()
							}

							ok = jw.tryToJoin(ctx, mt, innerIter4Row, joinResult)
							jw.innerCache = jw.innerCache[:0]
							trap = false
							if !ok {
								return
							}

						} else {
							// tested trap - 2019-9-2 9:50
							// there is next inner chunk, so add now innerRowsWithSameKey to jw.innerCache,
							// then get next innerRowsWithSameKey from new inner chunk
							trap = true
							for _, row := range rowsWithSameKey {
								jw.innerCache = append(jw.innerCache, row)
							}
							rowsWithSameKey, err = jw.innerRowsWithSameKey()
							if err != nil {
								return
							}
							continue
						}
					} else {
						// here are three situations (all tested):
						// (1) merge task is not the last one in outer chunk and jw.curInnerRow == jw.innerIter4Row.End() but jw.firstRow4Key != jw.innerIter4Row.End() - 4
						// (2) merge task is the last one in outer chunk and jw.curInnerRow == jw.innerIter4Row.End() but jw.firstRow4Key != jw.innerIter4Row.End() - 2
						// (3) merge task is the last one in outer chunk and jw.curInnerRow != jw.innerIter4Row.End() - 2
						var innerIter4Row chunk.Iterator
						if trap && len(jw.innerCache) > 0 {
							for _, row := range rowsWithSameKey {
								jw.innerCache = append(jw.innerCache, row)
							}

							innerIter4Row = chunk.NewIterator4Slice(jw.innerCache)
							innerIter4Row.Begin()
						} else {
							innerIter4Row = chunk.NewIterator4Slice(rowsWithSameKey)
							innerIter4Row.Begin()
						}

						ok = jw.tryToJoin(ctx, mt, innerIter4Row, joinResult)
						jw.innerCache = jw.innerCache[:0]
						trap = false
						if !ok {
							return
						}
					}
				} else {
					// here are one situation (tested):
					// (1) merge task is not the last one in outer chunk and innerRowWithSameKey is not also the last one in inner chunk
					// note: here may be the first merge task in the second outer chunk, but this join worker may not be the last join worker
					// which dealt the last merge task in the first outer chunk, so it is no needed to check firstTaskFlag and compare with jw.innerCache
					// but it needs to compare with jw.innerCache when the innerRowsWithSameKey is the first one of the second inner chunk
					var innerIter4Row chunk.Iterator
					// the innerRowsWithSameKey is the first one in the second inner chunk as trap = true
					if trap && len(jw.innerCache) > 0 {
						cmpCache := -1
						cmpCache = compareIOChunkRow(jw.compareFuncs, mt.outerRows[0], jw.innerCache[0], jw.outerJoinKeys, jw.innerJoinKeys)
						if cmpCache == 0 {
							for _, row := range rowsWithSameKey {
								jw.innerCache = append(jw.innerCache, row)
							}
							innerIter4Row = chunk.NewIterator4Slice(jw.innerCache)
							innerIter4Row.Begin()
						} else {
							innerIter4Row = chunk.NewIterator4Slice(rowsWithSameKey)
							innerIter4Row.Begin()
						}
					} else {
						innerIter4Row = chunk.NewIterator4Slice(rowsWithSameKey)
						innerIter4Row.Begin()
					}

					ok = jw.tryToJoin(ctx, mt, innerIter4Row, joinResult)
					jw.innerCache = jw.innerCache[:0]
					trap = false
					if !ok {
						return
					}
				}
			}

			if joinResult.chk.NumRows() > 0 {
				mt.joinResultCh <- joinResult
				ok, joinResult = jw.getNewJoinResult(ctx)
				if !ok {
					return
				}
			}
			close(mt.joinResultCh)
			break
		}
	}
}

func (jw *mergeJoinWorker) fetchNextInnerChunk(ctx context.Context) bool {
	select {
	case innerResult, ok := <-jw.innerFetchResultCh:
		if !ok && innerResult == nil {
			return false
		}

		jw.innerChunk = innerResult.fetchChunk
		jw.innerIter4Row = chunk.NewIterator4Chunk(jw.innerChunk)
		jw.firstRow4Key = jw.innerIter4Row.Begin()
		jw.curInnerRow = jw.innerIter4Row.Next()

		return true
	case <-ctx.Done():
		return false
	}
}

func (jw *mergeJoinWorker) tryToJoin(ctx context.Context, mt *mergeTask, innerIter4Row chunk.Iterator, joinResult *mergeJoinWorkerResult) bool {
	ok := true
	hasMatch := false
	for idx := 0; idx < len(mt.outerRows); {
		outerRow := mt.outerRows[idx]
		matched, _, err := jw.joiner.tryToMatch(outerRow, innerIter4Row, joinResult.chk)
		if err != nil {
			joinResult.err = errors.Trace(err)
			mt.joinResultCh <- joinResult
			return false
		}

		hasMatch = hasMatch || matched

		if innerIter4Row.Current() == innerIter4Row.End() {
			if !hasMatch {
				jw.joiner.onMissMatch(false, outerRow, joinResult.chk)
			}
			hasMatch = false
			innerIter4Row.Begin()
			idx++
		}

		if joinResult.chk.NumRows() >= jw.maxChunkSize {
			mt.joinResultCh <- joinResult
			ok, joinResult = jw.getNewJoinResult(ctx)
			if !ok {
				return false
			}
		}
	}
	return true
}

func (jw *mergeJoinWorker) innerRowsWithSameKey() ([]chunk.Row, error) {
	if jw.firstRow4Key == jw.innerIter4Row.End() {
		return nil, nil
	}
	var sameKeyRows []chunk.Row
	sameKeyRows = append(sameKeyRows, jw.firstRow4Key)
	for {
		selectedRow, err := jw.nextInnerChunkRow()
		// error happens or no more data.
		if err != nil || selectedRow == jw.innerIter4Row.End() {
			jw.firstRow4Key = jw.innerIter4Row.End()
			return sameKeyRows, err
		}
		compareResult := compareIOChunkRow(jw.compareFuncs, selectedRow, jw.firstRow4Key, jw.innerJoinKeys, jw.innerJoinKeys)
		if compareResult == 0 {
			sameKeyRows = append(sameKeyRows, selectedRow)
		} else {
			jw.firstRow4Key = selectedRow
			return sameKeyRows, nil
		}
	}
}

func (jw *mergeJoinWorker) nextInnerChunkRow() (chunk.Row, error) {
	for {
		if jw.curInnerRow == jw.innerIter4Row.End() {
			return jw.curInnerRow, nil
		}

		result := jw.curInnerRow
		jw.curInnerRow = jw.innerIter4Row.Next()

		return result, nil
	}
}

func (jw *mergeJoinWorker) getNewJoinResult(ctx context.Context) (bool, *mergeJoinWorkerResult) {
	joinResult := &mergeJoinWorkerResult{
		src: jw.joinChkResourceCh,
	}
	ok := true
	select {
	case <-ctx.Done():
		ok = false
	case <-jw.closeCh:
		ok = false
	case joinResult.chk, ok = <-jw.joinChkResourceCh:
	}

	return ok, joinResult
}

func (t *mergeJoinTable) init(ctx context.Context, chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.ctx = ctx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)

	err = Next(t.ctx, t.reader, t.curResult)
	if err != nil || t.curResult.NumRows() == 0 {
		return errors.Trace(err)
	}
	t.curRow = t.curIter.Begin()
	t.firstRow4Key = t.curRow
	t.curResultInUse = true
	t.curRow = t.curIter.Next()

	t.compareFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.compareFuncs = append(t.compareFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (ps *ParallelMergeJoinStrategy) getNextTask(ctx context.Context) *mergeTask {
	select {
	case task, ok := <-ps.mergeTaskCh:
		if ok {
			return task
		}
	case <-ctx.Done():
		return nil
	}
	return nil
}

func (t *mergeJoinTable) reAllocReaderResult() {
	//if !t.curResultInUse {
	//	// If "t.curResult" is not in use, we can just reuse it.
	//	t.curResult.Reset()
	//	return
	//}

	t.curResult = newFirstChunk(t.reader)
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curResult.Reset()
	t.curResultInUse = false
}

func compareIOChunkRow(cmpFuncs []chunk.CompareFunc, lhsRow, rhsRow chunk.Row, lhsKey, rhsKey []*expression.Column) int {
	for i := range lhsKey {
		cmp := cmpFuncs[i](lhsRow, lhsKey[i].Index, rhsRow, rhsKey[i].Index)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (t *mergeJoinTable) hasNullInJoinKey(row chunk.Row) bool {
	for _, col := range t.joinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}
