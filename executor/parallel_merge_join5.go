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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"golang.org/x/net/context"
	"time"
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

	stmtCtx      *stmtctx.StatementContext
	compareFuncs []chunk.CompareFunc
	joiner       joiner
	isOuterJoin  bool
	outerIdx     int

	innerTable *mergeJoinInnerTable
	outerTable *mergeJoinOuterTable

	memTracker *memory.Tracker

	curTask     *mergeTask
	mergeTaskCh <-chan *mergeTask

	closeCh            chan struct{}
	joinChkResourceChs []chan *chunk.Chunk
}

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

type mergeJoinOuterTable struct {
	mergeJoinTable
	filter   []expression.Expression
	selected []bool
}

type mergeJoinInnerTable struct {
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

	doneCh          chan<- *chunkRequest
	closedCh        chan<- int
	rowsWithSameKey []chunk.Row
}

type mergeTask struct {
	buildErr     error
	joinResultCh chan *mergeJoinWorkerResult

	outerRows     []chunk.Row
	outerSelected []bool
}

type innerFetchResult struct {
	err        error
	fetchChunk *chunk.Chunk
	memTracker *memory.Tracker
	chunkId    int
}

type outerFetchWorker struct {
	ctx        sessionctx.Context
	outerTable *mergeJoinOuterTable

	resultCh         chan<- *mergeTask // send to main thread
	joinWorkerTaskCh chan<- *mergeTask // send to join worker
	maxChunkSize     int
}

type innerFetchWorker struct {
	innerTable     *mergeJoinInnerTable
	doneCh         <-chan *chunkRequest
	closedCh       <-chan int
	innerResultChs []chan *innerFetchResult

	innerChunkCache []*chunk.Chunk
}

type chunkRequest struct {
	workerId       int
	requestChunkId int
}

type waitWorker struct {
	workerId int
	chunkId  int
}

type workerStatus struct {
	workerId    int
	lastChunkId int
	alive       bool
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	innerChunkCacheNum := 4
	//concurrency := 7

	closeCh := make(chan struct{})
	e.closeCh = closeCh

	// 1.init the channels between the join worker and the inner worker
	// each join worker has it's own channel to get the different inner chunk from inner worker
	innerFetchResultChs := make([]chan *innerFetchResult, concurrency)
	for j := 0; j < concurrency; j++ {
		innerFetchResultChs[j] = make(chan *innerFetchResult, 1)
	}
	// join worker send chunk request to the doneCh to get the next inner chunk from inner worker
	doneCh := make(chan *chunkRequest, concurrency)
	// join worker send it's worker id to the closedCh to tell the inner worker it is closed
	closedCh := make(chan int, concurrency)

	iw := e.newInnerFetchWorker(innerFetchResultChs, doneCh, closedCh)
	go iw.run(ctx, concurrency, innerChunkCacheNum)

	// 2.init channel between the join worker, main thread and outer worker
	// main thread get the merge task in taskCh from outer worker
	taskCh := make(chan *mergeTask, concurrency)
	e.mergeTaskCh = taskCh
	// main thread get the join result in joinChkResourceCh from join worker
	joinChkResourceChs := make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		joinChkResourceChs[i] = make(chan *chunk.Chunk, 1)
		joinChkResourceChs[i] <- newFirstChunk(e)
	}
	e.joinChkResourceChs = joinChkResourceChs

	// join worker get the merge task in mergeJoinWorkerMergeTaskCh from outer worker
	mergeJoinWorkerMergeTaskCh := make(chan *mergeTask, concurrency)
	for i := 0; i < concurrency; i++ {
		mw := e.newMergeJoinWorker(i, innerFetchResultChs[i], mergeJoinWorkerMergeTaskCh, joinChkResourceChs[i], doneCh, closedCh)
		go mw.run(ctx, i)
	}

	// outer worker send the merge task to taskCh and mergeJoinWorkerMergeTaskCh which are received by main thread and join worker respectively
	ow := e.newOuterFetchWorker(taskCh, mergeJoinWorkerMergeTaskCh)
	go ow.run(ctx)

	return nil
}

func (e *MergeJoinExec) newInnerFetchWorker(innerResultCh []chan *innerFetchResult, doneCh chan *chunkRequest, closedCh chan int) *innerFetchWorker {
	return &innerFetchWorker{
		innerResultChs: innerResultCh,
		innerTable:     e.innerTable,
		doneCh:         doneCh,
		closedCh:       closedCh,
	}
}

func (e *MergeJoinExec) newOuterFetchWorker(taskCh, mergeJoinWorkerMergeTaskCh chan *mergeTask) *outerFetchWorker {
	return &outerFetchWorker{
		outerTable:       e.outerTable,
		ctx:              e.ctx,
		resultCh:         taskCh,
		joinWorkerTaskCh: mergeJoinWorkerMergeTaskCh,
		maxChunkSize:     e.maxChunkSize,
	}
}

func (e *MergeJoinExec) newMergeJoinWorker(workerId int, innerFetchResultCh chan *innerFetchResult, mergeJoinWorkerMergeTaskCh chan *mergeTask, joinChkResourceCh chan *chunk.Chunk, doneCh chan<- *chunkRequest, closedCh chan<- int) *mergeJoinWorker {
	return &mergeJoinWorker{
		closeCh:            e.closeCh,
		innerFetchResultCh: innerFetchResultCh,
		mergeTaskCh:        mergeJoinWorkerMergeTaskCh,
		joinChkResourceCh:  joinChkResourceCh,
		joiner:             e.joiner,
		maxChunkSize:       e.maxChunkSize,
		outerJoinKeys:      e.outerTable.joinKeys,
		innerJoinKeys:      e.innerTable.joinKeys,
		compareFuncs:       e.compareFuncs,
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
		mt.outerRows = outerRows

		// joinResultCh is the channel between the join worker and main thread
		// join worker get the merge task and send the join result to task's joinResultCh, then main thread get the join result
		joinResultCh := make(chan *mergeJoinWorkerResult)
		mt.joinResultCh = joinResultCh

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
func (iw *innerFetchWorker) run(ctx context.Context, concurrency int, innerChunkCacheNum int) {
	defer func() {
		for _, innerResultCh := range iw.innerResultChs {
			close(innerResultCh)
			for range innerResultCh {
			}
		}
	}()

	fetchResult := &innerFetchResult{}
	err := iw.innerTable.init(ctx, newFirstChunk(iw.innerTable.reader))
	if err != nil {
		fetchResult.err = err
		for i := 0; i < concurrency; i++ {
			iw.innerResultChs[i] <- fetchResult
		}
		return
	}

	// send the first inner chunk to all join worker
	fetchResult.fetchChunk = iw.innerTable.curResult
	fetchResult.err = err
	fetchResult.chunkId = 0
	for i := 0; i < concurrency; i++ {
		iw.innerResultChs[i] <- fetchResult
	}

	var (
		closedWorkerIndex int
		chunkRequest      *chunkRequest
		ok                bool
		aliveWorkerIndex  []int
		chunkIds          []int // record now chunk ids in innerChunkCache
		waitGroup         []*waitWorker
		done              bool // whether there is more inner chunk
	)

	workers := make([]*workerStatus, concurrency)
	aliveNum := concurrency
	lastChunkId := 0 // record the last inner chunk's id in innerChunkCache

	for j := 0; j < concurrency; j++ {
		aliveWorkerIndex = append(aliveWorkerIndex, j)
		workers[j] = &workerStatus{
			workerId:    j,
			alive:       true,
			lastChunkId: 0,
		}
	}

	for {
		// a new chunk is needed, otherwise the columns in joinWorker's outerRowsWithSameKey will be cleared
		iw.innerTable.curResult = newFirstChunk(iw.innerTable.reader)

		err = Next(ctx, iw.innerTable.reader, iw.innerTable.curResult)
		if err != nil {
			return
		}

		if iw.innerTable.curResult.NumRows() == 0 && len(iw.innerChunkCache) == 0 {
			return
		}

		if iw.innerTable.curResult.NumRows() != 0 && len(iw.innerChunkCache) < innerChunkCacheNum {
			iw.innerChunkCache = append(iw.innerChunkCache, iw.innerTable.curResult)
			lastChunkId += 1
			chunkIds = append(chunkIds, lastChunkId)
		} else {
			break
		}
	}

	for {
		select {
		case <-ctx.Done():
			ok = false
		case chunkRequest, ok = <-iw.doneCh:
			if chunkRequest.requestChunkId <= chunkIds[len(chunkIds)-1] {
				// 情况1：3 号 worker 等待获取第 5 个 chunk，此时 0 号 worker 处理完第一个 chunk 后，需要获取第 2 个 chunk，
				// 从而触发去除第一个 chunk，获取第 5 个 chunk
				fetchResult = &innerFetchResult{}
				var chunkIndex int
				for index, chunkId := range chunkIds {
					if chunkId == chunkRequest.requestChunkId {
						chunkIndex = index
						break
					}
				}
				fetchResult.fetchChunk = iw.innerChunkCache[chunkIndex]
				fetchResult.chunkId = chunkRequest.requestChunkId
				iw.innerResultChs[chunkRequest.workerId] <- fetchResult
				workers[chunkRequest.workerId].lastChunkId = chunkRequest.requestChunkId

				if !iw.checkWaitWorker(ctx, aliveWorkerIndex, workers, waitGroup, chunkIds, &done) {
					return
				}
			} else {
				// if has next inner chunk
				if done {
					fetchResult = &innerFetchResult{}
					fetchResult.chunkId = chunkRequest.requestChunkId
					iw.innerResultChs[chunkRequest.workerId] <- fetchResult
				} else {
					// 假设有两个 worker 还在工作，分别为 0 号和 3 号，chunkIds 为[1,2,3,4]，0 号正在处理的是 1 号 chunk，而 3 号 worker
					// 需要获取第 5 个 chunk，所以 3 号 worker 需要等待 0 号 worker 获取 2 号 chunk 或者 0 号直接结束（两种情况）
					waitWorker := &waitWorker{
						workerId: chunkRequest.workerId,
						chunkId:  chunkRequest.requestChunkId,
					}
					waitGroup = append(waitGroup, waitWorker)

					if !iw.checkWaitWorker(ctx, aliveWorkerIndex, workers, waitGroup, chunkIds, &done) {
						return
					}
				}
			}
		case closedWorkerIndex, ok = <-iw.closedCh:
			workers[closedWorkerIndex].alive = false
			aliveNum -= 1

			var deleteIndex int
			for num, id := range aliveWorkerIndex {
				if id == closedWorkerIndex {
					deleteIndex = num
					break
				}
			}
			aliveWorkerIndex = append(aliveWorkerIndex[:deleteIndex], aliveWorkerIndex[deleteIndex+1:]...)

			// 情况 2：3 号 worker 还在等待第 5 个 chunk，此时 0 号 worker 处理第 1 个 chunk 后直接退出了，那么后面就一直阻塞了，因此
			// 此时需要判断是否有等待的 worker 且是否能移除 innerCache 中的第一个 chunk，为下一个 chunk 腾出位置
			if len(waitGroup) > 0 {
				if !iw.checkWaitWorker(ctx, aliveWorkerIndex, workers, waitGroup, chunkIds, &done) {
					return
				}
			}
		}

		if !ok || aliveNum == 0 {
			return
		}
	}
}

func (iw *innerFetchWorker) checkWaitWorker(ctx context.Context, aliveWorkerIndex []int, workers []*workerStatus, waitGroup []*waitWorker, chunkIds []int, done *bool) bool {
	flag := true
	chunkMaxId := chunkIds[len(chunkIds)-1]
	for _, index := range aliveWorkerIndex {
		if workers[index].lastChunkId < chunkIds[0] {
			flag = false
			break
		}
	}
	if flag {
		iw.innerChunkCache = iw.innerChunkCache[1:]
		chunkIds = append(chunkIds[:0], chunkIds[1:]...)

		if iw.innerTable.curResult.NumRows() != 0 {
			iw.innerChunkCache = append(iw.innerChunkCache, iw.innerTable.curResult)
			chunkMaxId += 1
			chunkIds = append(chunkIds, chunkMaxId)

			for _, waitedWorker := range waitGroup {
				if waitedWorker.chunkId == chunkMaxId {
					fetchResult := &innerFetchResult{}
					fetchResult.fetchChunk = iw.innerTable.curResult
					iw.innerResultChs[waitedWorker.workerId] <- fetchResult
				}
			}
			waitGroup = waitGroup[:0]

			// get next inner chunk
			iw.innerTable.curResult = newFirstChunk(iw.innerTable.reader)

			err := Next(ctx, iw.innerTable.reader, iw.innerTable.curResult)
			if err != nil {
				return false
			}

			if iw.innerTable.curResult.NumRows() == 0 && len(iw.innerChunkCache) == 0 {
				return false
			}

			if iw.innerTable.curResult.NumRows() == 0 {
				*done = true
			}
		}
	}
	return true
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
	var mt *mergeTask
	chunkId := 1

	for {
		select {
		case <-ctx.Done():
			ok = false
		case <-jw.closeCh:
			ok = false
		case mt, ok = <-jw.mergeTaskCh:
		}

		if !ok {
			jw.closedCh <- i
			return
		}

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
					chunkRequest := &chunkRequest{
						workerId:       i,
						requestChunkId: chunkId,
					}
					jw.doneCh <- chunkRequest
					// (2) then get the next inner chunk, if there is no next inner chunk then break to get the next task
					if !jw.fetchNextInnerChunk(ctx) {
						close(mt.joinResultCh)
						break
					}
					chunkId += 1
					rowsWithSameKey, err = jw.innerRowsWithSameKey()
					if err != nil {
						return
					}
				}
				continue
			}

			if cmpResult < 0 {
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
			} else {
				// note: when jw.curInnerRow == jw.innerIter4Row.End(), jw.firstRow4Key may not be equal to jw.innerIter4Row.End()
				// if jw.firstRow4Key is the last row in now inner chunk, so there is still last innerRowsWithSameKey which just contain the last row
				if jw.curInnerRow == jw.innerIter4Row.End() && jw.firstRow4Key == jw.innerIter4Row.End() {
					chunkRequest := &chunkRequest{
						workerId:       i,
						requestChunkId: chunkId,
					}
					jw.doneCh <- chunkRequest

					innerIter4Row := chunk.NewIterator4Slice(rowsWithSameKey)
					innerIter4Row.Begin()

					ok = jw.tryToJoin(ctx, mt, innerIter4Row, joinResult)
					if !ok {
						return
					}

					if jw.fetchNextInnerChunk(ctx) {
						chunkId += 1
						rowsWithSameKey, err = jw.innerRowsWithSameKey()
						if err != nil {
							return
						}
						continue
					}
				} else {
					innerIter4Row := chunk.NewIterator4Slice(rowsWithSameKey)
					innerIter4Row.Begin()

					ok = jw.tryToJoin(ctx, mt, innerIter4Row, joinResult)
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
		if !ok || innerResult == nil || innerResult.fetchChunk == nil {
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

func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()

	var err error
	for {
		if e.curTask == nil {
			e.curTask = e.getNextTask(ctx)
			if e.curTask == nil {
				break
			}

			if e.curTask.buildErr != nil {
				return e.curTask.buildErr
			}
		}

		joinResult, ok := <-e.curTask.joinResultCh
		//curTask process complete, we need getNextTask, so set curTask = nil
		if !ok {
			e.curTask = nil
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

func (e *MergeJoinExec) getNextTask(ctx context.Context) *mergeTask {
	select {
	case task, ok := <-e.mergeTaskCh:
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

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	close(e.closeCh)

	for _, joinChkResourceCh := range e.joinChkResourceChs {
		close(joinChkResourceCh)
		for range joinChkResourceCh {
		}
	}
	return errors.Trace(e.baseExecutor.Close())
}
