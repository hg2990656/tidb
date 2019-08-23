package executor

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"golang.org/x/net/context"
	"sync"
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

	workerWg     *sync.WaitGroup
	stmtCtx      *stmtctx.StatementContext
	compareFuncs []chunk.CompareFunc
	joiner       joiner // e.joiner.tryToMatch & e.joiner.onMissMatch
	isOuterJoin  bool
	outerIdx int

	innerTable mergeJoinInnerTable
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
	innerChunk *chunk.Chunk
	innerIter4Row      chunk.Iterator

	innerCache []chunk.Row

	innerTable mergeJoinInnerTable

	outerJoinKeys []*expression.Column
	innerJoinKeys      []*expression.Column

	compareFuncs []chunk.CompareFunc

}

type mergeTask struct {
	cmp          int
	buildErr     error
	waitGroup    *sync.WaitGroup
	joinResultCh chan *mergeJoinWorkerResult

	innerRows []chunk.Row

	outerRows     []chunk.Row
	outerSelected []bool

	lastTaskFlag bool
	firstTaskFlag bool
}

type outerFetchResult struct {
	err        error
	selected   []bool
	fetchRow   []chunk.Row
	memTracker *memory.Tracker
}

type innerFetchResult struct {
	err        error
	//fetchRow   []chunk.Row
	fetchChunk *chunk.Chunk
	memTracker *memory.Tracker
}

type outerFetchWorker struct {
	ctx                sessionctx.Context
	outerTable         *mergeJoinOuterTable
	outerFetchResultCh chan<- *outerFetchResult
	resultCh           chan<- *mergeTask
	innerCh            chan<- *mergeTask
	maxChunkSize       int
}

//type innerFetchWorker struct {
//	innerTable    *mergeJoinInnerTable
//	innerResultCh chan<- *innerFetchResult
//}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	//concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	concurrency := 1

	closeCh := make(chan struct{})
	e.closeCh = closeCh

	taskCh := make(chan *mergeTask, concurrency)
	e.mergeTaskCh = taskCh

	joinChkResourceChs := make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		joinChkResourceChs[i] = make(chan *chunk.Chunk, 1)
		joinChkResourceChs[i] <- newFirstChunk(e)
	}
	e.joinChkResourceChs = joinChkResourceChs

	//e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	//e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	//innerFetchResultCh := make(chan *innerFetchResult)
	//e.workerWg.Add(1)
	//iw := e.newInnerFetchWorker(innerFetchResultCh)
	//go iw.run(ctx, e.workerWg)

	mergeJoinWorkerMergeTaskCh := make(chan *mergeTask, concurrency)
	//e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		mw := e.newMergeJoinWorker(i, mergeJoinWorkerMergeTaskCh, joinChkResourceChs[i])
		go mw.run(ctx, e.workerWg)
	}

	outerFetchResultCh := make(chan *outerFetchResult)
	//innerCh := make(chan *mergeTask, concurrency)
	//e.workerWg.Add(1)
	ow := e.newOuterFetchWorker(outerFetchResultCh, taskCh, mergeJoinWorkerMergeTaskCh)
	go ow.run(ctx, e.workerWg)

	return nil
}

func (e *MergeJoinExec) newOuterFetchWorker(outerFetchResultCh chan<- *outerFetchResult, resultCh, innerCh chan *mergeTask) *outerFetchWorker {
	return &outerFetchWorker{
		outerTable:         e.outerTable,
		outerFetchResultCh: outerFetchResultCh,
		ctx:                e.ctx,
		resultCh:           resultCh,
		innerCh:            innerCh,
		maxChunkSize:       e.maxChunkSize,
	}
}

//func (e *MergeJoinExec) newInnerFetchWorker(innerResultCh chan<- *innerFetchResult) *innerFetchWorker {
//	return &innerFetchWorker{
//		innerResultCh: innerResultCh,
//		innerTable:    e.innerTable,
//	}
//}

func (e *MergeJoinExec) newMergeJoinWorker(workerId int, mergeTaskCh chan *mergeTask, joinChkResourceCh chan *chunk.Chunk) *mergeJoinWorker {
	return &mergeJoinWorker{
		closeCh:           e.closeCh,
		mergeTaskCh:       mergeTaskCh,
		joinChkResourceCh: joinChkResourceCh,
		joiner:            e.joiner,
		maxChunkSize:      e.maxChunkSize,
		innerTable:    e.innerTable,
		outerJoinKeys: e.outerTable.joinKeys,
		innerJoinKeys: e.innerTable.joinKeys,
		compareFuncs:  e.compareFuncs,
	}
}

func (ow *outerFetchWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		close(ow.resultCh)
		close(ow.innerCh)
		//wg.Done()
	}()

	err := ow.outerTable.init(ctx, newFirstChunk(ow.outerTable.reader))
	if err != nil {
		return
	}
	endRow := ow.outerTable.curIter.End()

	waitGroup := new(sync.WaitGroup)
	joinResultCh := make(chan *mergeJoinWorkerResult)

	for {
		endRow = ow.outerTable.curIter.End()
		if ow.outerTable.curRow == endRow {
		//if ow.outerTable.curRow.Idx() == (ow.outerTable.curIter.Len() - 1) {
			err := ow.fetchNextOuterChunk(ctx)
			if err != nil || ow.outerTable.curResult.NumRows() == 0 {
				break
			}
		}
		// generate merge task
		outerRows, err := ow.outerTable.getSameKeyRows()
		if err != nil {
			break
		}
		mt := &mergeTask{waitGroup: waitGroup}
		mt.outerRows = outerRows
		if ow.outerTable.curRow == ow.outerTable.curIter.End() {
			mt.lastTaskFlag = true
		}

		mt.joinResultCh = joinResultCh

		waitGroup.Add(1)

		ow.innerCh <- mt
		ow.resultCh <- mt
	}

	go func() {
		waitGroup.Wait()
		close(joinResultCh)
	}()
}

func (t *mergeJoinTable) getSameKeyRows() ([]chunk.Row, error){
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}
	t.sameKeyRows = t.sameKeyRows[:0]
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextChunkRow2()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, err
		}
		compareResult := compareIOChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return t.sameKeyRows, nil
		}
	}
}

func (t *mergeJoinTable) nextChunkRow2() (chunk.Row, error){
	for {
		if t.curRow == t.curIter.End() {
			return t.curRow, nil
		}

		result := t.curRow
		t.curResultInUse = true
		t.curRow = t.curIter.Next()

		return result, nil
	}
}

func (t *mergeJoinTable) rowsWithSameKey() ([]chunk.Row, error) {
	// no more data.
	if t.firstRow4Key.Idx() == (t.curIter.Len() - 1) {
		t.sameKeyRows = t.sameKeyRows[:0]
		t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
		t.curRow = t.firstRow4Key
		return t.sameKeyRows, nil
	}
	t.sameKeyRows = t.sameKeyRows[:0]
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	flag := false
	for {
		selectedRow, err := t.nextChunkRow()
		// error happens or no more data.
		if err != nil || flag {
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, err
		}
		compareResult := compareIOChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
			if selectedRow.Idx() == (t.curIter.Len() - 1) {
				flag = true
			}
		} else {
			t.firstRow4Key = selectedRow
			return t.sameKeyRows, nil
		}
	}
}

func (t *mergeJoinTable) nextChunkRow() (chunk.Row, error) {
	for {
		if t.curRow.Idx() == (t.curIter.Len() - 1) {
			return t.curRow, nil
		}

		result := t.curRow
		t.curResultInUse = true
		t.curRow = t.curIter.Next()

		return result, nil
	}
}

func (t *mergeJoinTable) hasNullInJoinKey(row chunk.Row) bool{
	for _, col := range t.joinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

func (ow *outerFetchWorker) fetchNextOuterChunk(ctx context.Context) (err error) {
	ow.outerTable.reallocReaderResult()

	//err = ow.outerTable.reader.Next(ctx, chunk.NewRecordBatch(ow.outerTable.curResult))
	err = Next(ctx, ow.outerTable.reader, ow.outerTable.curResult)
	if err != nil {
		return err
	}

	ow.outerTable.curIter.Begin()
	ow.outerTable.selected, err = expression.VectorizedFilter(ow.ctx, ow.outerTable.filter, ow.outerTable.curIter, ow.outerTable.selected)
	if err != nil {
		return err
	}
	ow.outerTable.curRow = ow.outerTable.curIter.Begin()
	return nil
}

//func (iw *innerFetchWorker) run(ctx context.Context, wg *sync.WaitGroup) {
//	defer func() {
//		close(iw.innerResultCh)
//		wg.Done()
//	}()
//
//	//fetchResult := &outerFetchResult{}
//	err := iw.innerTable.init(ctx, iw.innerTable.reader.newFirstChunk())
//	if err != nil {
//		//fetchResult.err = err
//		//ow.outerFetchResultCh <- fetchResult
//		return
//	}
//
//	error := iw.fetchNextInnerChunk(ctx)
//	if error != nil {
//		return
//	}
//
//	fetchResult := &innerFetchResult{}
//	fetchResult.fetchChunk = iw.innerTable.curResult
//	fetchResult.err = error
//
// 	iw.innerResultCh <- fetchResult
//}

//func (iw *innerFetchWorker) fetchNextInnerChunk(ctx context.Context) (err error) {
//	err = iw.innerTable.reader.Next(ctx, chunk.NewRecordBatch(iw.innerTable.curResult))
//	if err != nil {
//		return err
//	}
//
//	iw.innerTable.curIter.Begin()
//	iw.innerTable.curRow = iw.innerTable.curIter.Begin()
//	return nil
//}

func (jw *mergeJoinWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	//defer func(){
	//	wg.Done()
	//}()

	ok, joinResult := jw.getNewJoinResult(ctx)
	if !ok {
		return
	}

	err := jw.innerTable.init(ctx, newFirstChunk(jw.innerTable.reader))
	if err != nil {
		return
	}

	rowsWithSameKey, err := jw.innerTable.getSameKeyRows()

	var mt *mergeTask
	var s int
	// 1.get merge task from outerFetchWorker
	for {
		select {
		case <-ctx.Done():
			ok = false
			s = 1
		case <-jw.closeCh:
			ok = false
			s = 2
		case mt, ok = <-jw.mergeTaskCh:
			s = 3
		}

		if !ok {
			fmt.Println("123", s)
			return
		}

		trap := false
		hasMatch := false

		// 2.use for{} to get the sameKeyGroup rows from inner table chunk and compare with outer table rows in merge task
		for {
			cmpResult := -1

			cmpResult = compareIOChunkRow(jw.innerTable.compareFuncs, mt.outerRows[0], rowsWithSameKey[0], jw.outerJoinKeys, jw.innerJoinKeys)

			if cmpResult > 0 {
				rowsWithSameKey, err = jw.innerTable.getSameKeyRows()
				if err != nil {
					return
				}
				if len(rowsWithSameKey) == 0 {
					err := jw.fetchNextInnerChunk(ctx)
					if err != nil || jw.innerTable.curResult.NumRows() == 0 {
						return
					}
					trap = true
					rowsWithSameKey, err = jw.innerTable.getSameKeyRows()
					if err != nil {
						return
					}
				}
				continue
			}

			// 3.if cmpResult = 0 then tryToMatch , cmpResult < 0 then onMissMatch, cmpResult > 0 then get the next sameKeyGroup rows from inner chunk
			if cmpResult < 0 {
				if trap {
					// do join with innerCache
					cmpCache := -1
					if len(jw.innerCache) > 0 {
						cmpCache = compareIOChunkRow(jw.innerTable.compareFuncs, mt.outerRows[0], jw.innerCache[0], jw.outerJoinKeys, jw.innerJoinKeys)
						if cmpCache == 0 {
							// do join with
							for _, row := range rowsWithSameKey {
								jw.innerCache = append(jw.innerCache, row)
							}
							innerIter4Row := chunk.NewIterator4Slice(jw.innerCache)
							innerIter4Row.Begin()
							for _, outerRow := range mt.outerRows {
								_, _, err := jw.joiner.tryToMatch(outerRow, innerIter4Row, joinResult.chk)
								if err != nil {
									joinResult.err = errors.Trace(err)
									mt.joinResultCh <- joinResult
									return
								}

								if joinResult.chk.NumRows() >= jw.maxChunkSize {
									mt.joinResultCh <- joinResult
									ok, joinResult = jw.getNewJoinResult(ctx)
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

							return
						}
					}
				}
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
				//cmp := compareIOChunkRow(jw.innerTable.compareFuncs, rowsWithSameKey[0], jw.innerTable.curRow, jw.outerJoinKeys, jw.innerJoinKeys)
				if mt.lastTaskFlag || jw.innerTable.curRow == jw.innerTable.curIter.End() {
					//jw.innerCache = jw.innerCache[:0]
					for _, row := range rowsWithSameKey {
						jw.innerCache = append(jw.innerCache, row)
					}
					err := jw.fetchNextInnerChunk(ctx)
					if err != nil || jw.innerTable.curResult.NumRows() == 0 {
						// do join with rowsWithSameKey
						innerIter4Row := chunk.NewIterator4Slice(rowsWithSameKey)
						innerIter4Row.Begin()

						for _, outerRow := range mt.outerRows {
							matched, _, err := jw.joiner.tryToMatch(outerRow, innerIter4Row, joinResult.chk)
							if err != nil {
								joinResult.err = errors.Trace(err)
								mt.joinResultCh <- joinResult
								return
							}

							hasMatch = hasMatch || matched

							if innerIter4Row.Current() == innerIter4Row.End() {
								if !hasMatch {
									jw.joiner.onMissMatch(false, outerRow, joinResult.chk)
								}
								hasMatch = false
								innerIter4Row.Begin()
							}

							if joinResult.chk.NumRows() >= jw.maxChunkSize {
								mt.joinResultCh <- joinResult
								ok, joinResult = jw.getNewJoinResult(ctx)
								if !ok {
									return
								}
								//jw.innerCache = jw.innerCache[:0]
							}
						}

					} else {
						trap = true
						rowsWithSameKey, err = jw.innerTable.rowsWithSameKey()
						if err != nil {
							return
						}
						continue
					}
				} else {
					var joinRows []chunk.Row
					//if mt.firstTaskFlag {
					if len(jw.innerCache) > 0 {
						cmpCache := -1
						cmpCache = compareIOChunkRow(jw.innerTable.compareFuncs, mt.outerRows[0], jw.innerCache[0], jw.outerJoinKeys, jw.innerJoinKeys)
						if cmpCache == 0 {
							for _, row := range jw.innerCache {
								joinRows = append(joinRows, row)
							}
						} else {
							jw.innerCache = jw.innerCache[:0]
						}
					}
					//}
					for _, row := range rowsWithSameKey {
						joinRows = append(joinRows, row)
					}
					innerIter4Row := chunk.NewIterator4Slice(joinRows)
					innerIter4Row.Begin()

					for _, outerRow := range mt.outerRows {
						matched, _, err := jw.joiner.tryToMatch(outerRow, innerIter4Row, joinResult.chk)
						if err != nil {
							joinResult.err = errors.Trace(err)
							mt.joinResultCh <- joinResult
							return
						}

						hasMatch = hasMatch || matched

						if innerIter4Row.Current() == innerIter4Row.End() {
							if !hasMatch {
								jw.joiner.onMissMatch(false, outerRow, joinResult.chk)
							}
							hasMatch = false
							innerIter4Row.Begin()
						}

						if joinResult.chk.NumRows() >= jw.maxChunkSize {
							mt.joinResultCh <- joinResult
							ok, joinResult = jw.getNewJoinResult(ctx)
							if !ok {
								return
							}
							//jw.innerCache = jw.innerCache[:0]
						}
					}
				}
			}

			if joinResult.chk.NumRows() > 0 {
				mt.joinResultCh <- joinResult
				ok, joinResult = jw.getNewJoinResult(ctx)
				if !ok {
					return
				}
				mt.waitGroup.Done()
				break
			}
		}
	}
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

func (jw *mergeJoinWorker) fetchNextInnerChunk(ctx context.Context) (err error) {
	jw.innerTable.reallocReaderResult()
	//err = jw.innerTable.reader.Next(ctx, chunk.NewRecordBatch(jw.innerTable.curResult))
	err = Next(ctx, jw.innerTable.reader, jw.innerTable.curResult)
	if err != nil {
		return err
	}

	jw.innerTable.curIter.Begin()

	jw.innerTable.curRow = jw.innerTable.curIter.Begin()

	jw.innerTable.firstRow4Key = jw.innerTable.curIter.Begin()
	return nil
}

func (jw *mergeJoinWorker) getInnerSameKeyRows() ([]chunk.Row, error){
	// no more data.
	if jw.innerTable.firstRow4Key == jw.innerTable.curIter.End() {
		return nil, nil
	}
	jw.innerTable.sameKeyRows = jw.innerTable.sameKeyRows[:0]
	jw.innerTable.sameKeyRows = append(jw.innerTable.sameKeyRows, jw.innerTable.firstRow4Key)

	flag := false
	for {
		selectedRow, err := jw.innerTable.nextChunkRow()
		// error happens or no more data.
		if err != nil || flag {
			jw.innerTable.firstRow4Key = jw.innerTable.curIter.End()
			return jw.innerTable.sameKeyRows, err
		}
		compareResult := compareIOChunkRow(jw.innerTable.compareFuncs, selectedRow, jw.innerTable.firstRow4Key, jw.innerTable.joinKeys, jw.innerTable.joinKeys)
		if compareResult == 0 {
			jw.innerTable.sameKeyRows = append(jw.innerTable.sameKeyRows, selectedRow)
			if selectedRow.Idx() == (jw.innerTable.curIter.Len() - 1) {
				flag = true
			}
		} else {
			jw.innerTable.firstRow4Key = selectedRow
			return jw.innerTable.sameKeyRows, nil
		}
	}
}

func (jw *mergeJoinWorker) fetchNextInnerRows(ctx context.Context) bool{
	select {
	case innerResult, ok := <-jw.innerFetchResultCh:
		if !ok {
			//jw.innerRows = make([]chunk.Row, 0)
			//return true
			return false
		}

		jw.innerChunk = innerResult.fetchChunk
		return true
	case <-ctx.Done():
		return false
	}
}

func (jw *mergeJoinWorker) getNewJoinResult(ctx context.Context) (bool, *mergeJoinWorkerResult){
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
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	//t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
	t.compareFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.compareFuncs = append(t.compareFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (t *mergeJoinTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			t.reallocReaderResult()
			//oldMemUsage := t.curResult.MemoryUsage()
			//err := t.reader.Next(t.ctx, chunk.NewRecordBatch(t.curResult))
			err := Next(t.ctx, t.reader, t.curResult)
			numRows := t.curResult.NumRows()
			// error happens or no more data.
			if err != nil || numRows == 0 {
				t.curRow = t.curIter.End()
				return t.curRow, errors.Trace(err)
			}
			//newMemUsage := t.curResult.MemoryUsage()
			//t.memTracker.Consume(newMemUsage - oldMemUsage)
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

func (t *mergeJoinTable) reallocReaderResult() {
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

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	//e.memTracker.Detach()
	//e.memTracker = nil

	close(e.closeCh)
	//e.workerWg.Wait()

	for _, joinChkResourceCh := range e.joinChkResourceChs {
		close(joinChkResourceCh)
		for range joinChkResourceCh {
		}
	}
	return errors.Trace(e.baseExecutor.Close())
}
