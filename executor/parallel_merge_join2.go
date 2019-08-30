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

	stmtCtx      *stmtctx.StatementContext
	compareFuncs []chunk.CompareFunc
	joiner       joiner // e.joiner.tryToMatch & e.joiner.onMissMatch
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

	innerChunk    *chunk.Chunk // like curResult
	innerIter4Row *chunk.Iterator4Chunk
	curInnerRow   chunk.Row
	firstRow4Key  chunk.Row
	sameKeyRows   []chunk.Row
	compareFuncs  []chunk.CompareFunc

	innerCache []chunk.Row

	outerJoinKeys []*expression.Column
	innerJoinKeys []*expression.Column

	doneCh chan<- bool
	rowsWithSameKey []chunk.Row
}

type mergeTask struct {
	cmp          int
	buildErr     error
	waitGroup    *sync.WaitGroup
	joinResultCh chan *mergeJoinWorkerResult

	innerRows []chunk.Row

	outerRows     []chunk.Row
	outerSelected []bool

	lastTaskFlag  bool
	firstTaskFlag bool
}

type outerFetchResult struct {
	err        error
	selected   []bool
	fetchRow   []chunk.Row
	memTracker *memory.Tracker
}

type innerFetchResult struct {
	err error
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

type innerFetchWorker struct {
	innerTable    *mergeJoinInnerTable
	innerResultCh chan<- *innerFetchResult
	doneCh        <-chan bool
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency

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

	innerFetchResultCh := make(chan *innerFetchResult, concurrency)
	doneCh := make(chan bool, concurrency)
	iw := e.newInnerFetchWorker(innerFetchResultCh, doneCh)
	go iw.run(ctx, concurrency)

	mergeJoinWorkerMergeTaskCh := make(chan *mergeTask, concurrency)
	for i := 0; i < concurrency; i++ {
		mw := e.newMergeJoinWorker(i, innerFetchResultCh, mergeJoinWorkerMergeTaskCh, joinChkResourceChs[i], doneCh)
		go mw.run(ctx, i)
	}

	outerFetchResultCh := make(chan *outerFetchResult)
	ow := e.newOuterFetchWorker(outerFetchResultCh, taskCh, mergeJoinWorkerMergeTaskCh)
	go ow.run(ctx)

	return nil
}

func (e *MergeJoinExec) newOuterFetchWorker(outerFetchResultCh chan<- *outerFetchResult, taskCh, mergeJoinWorkerMergeTaskCh chan *mergeTask) *outerFetchWorker {
	return &outerFetchWorker{
		outerTable:         e.outerTable,
		outerFetchResultCh: outerFetchResultCh,
		ctx:                e.ctx,
		resultCh:           taskCh,
		innerCh:            mergeJoinWorkerMergeTaskCh,
		maxChunkSize:       e.maxChunkSize,
	}
}

func (e *MergeJoinExec) newInnerFetchWorker(innerResultCh chan<- *innerFetchResult, doneCh chan bool) *innerFetchWorker {
	return &innerFetchWorker{
		innerResultCh: innerResultCh,
		innerTable:    e.innerTable,
		doneCh:        doneCh,
	}
}

func (e *MergeJoinExec) newMergeJoinWorker(workerId int, innerFetchResultCh chan *innerFetchResult, mergeJoinWorkerMergeTaskCh chan *mergeTask, joinChkResourceCh chan *chunk.Chunk, doneCh chan<- bool) *mergeJoinWorker {
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
	}
}

func (ow *outerFetchWorker) run(ctx context.Context) {
	defer func() {
		close(ow.resultCh)
		close(ow.innerCh)
	}()

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
		// generate merge task
		outerRows, err := ow.outerTable.getSameKeyRows()
		if err != nil {
			break
		}

		mt := &mergeTask{}
		mt.outerRows = outerRows

		joinResultCh := make(chan *mergeJoinWorkerResult)
		mt.joinResultCh = joinResultCh

		if ow.outerTable.curRow == ow.outerTable.curIter.End() {
			mt.lastTaskFlag = true
		}

		if finished := ow.pushToChan(ctx, mt, ow.innerCh); finished {
			return
		}

		if finished := ow.pushToChan(ctx, mt, ow.resultCh); finished {
			return
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

func (t *mergeJoinTable) getSameKeyRows() ([]chunk.Row, error) {
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}
	var sameKeyRows []chunk.Row
	//t.sameKeyRows = t.sameKeyRows[:0]
	//t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	sameKeyRows = append(sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextChunkRow()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			//return t.sameKeyRows, err
			//sameKeyRows = append(sameKeyRows, )
			return sameKeyRows, err
		}
		compareResult := compareIOChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			//t.sameKeyRows = append(t.sameKeyRows, selectedRow)
			sameKeyRows = append(sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			//return t.sameKeyRows, nil
			return sameKeyRows, nil
		}
	}
}

func (t *mergeJoinTable) nextChunkRow() (chunk.Row, error) {
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

func (t *mergeJoinTable) hasNullInJoinKey(row chunk.Row) bool {
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

// innerWorker run
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

	// send first inner chunk
	fetchResult.fetchChunk = iw.innerTable.curResult
	fetchResult.err = err

	for i := 0; i < concurrency; i++ {
		iw.innerResultCh <- fetchResult
	}

	var count int
	var ok bool
	var done bool

	for {
		select {
		case <-ctx.Done():
			ok = false
		case done, ok = <-iw.doneCh:
		}

		if !ok {
			return
		}

		if done {
			count = count + 1
			if count == concurrency {
				fetchResult = &innerFetchResult{}
				iw.innerTable.curResult = newFirstChunk(iw.innerTable.reader)
				err := iw.fetchNextInnerChunkRows(ctx)
				if err != nil {
					return
				}
				if iw.innerTable.curResult.NumRows() > 0 {
					fetchResult.fetchChunk = iw.innerTable.curResult
					fetchResult.err = err

					for i := 0; i < concurrency; i++ {
						iw.innerResultCh <- fetchResult
					}
				}

				if err != nil || iw.innerTable.curResult.NumRows() == 0 {
					return
				}

				count = 0
			}
		}
	}
}

// inner worker fetchNextInnerChunkRows
func (iw *innerFetchWorker) fetchNextInnerChunkRows(ctx context.Context) (err error) {
	err = Next(ctx, iw.innerTable.reader, iw.innerTable.curResult)
	if err != nil {
		return err
	}

	iw.innerTable.curIter.Begin()
	iw.innerTable.curRow = iw.innerTable.curIter.Begin()
	return nil
}

// merge join worker run
func (jw *mergeJoinWorker) run(ctx context.Context, i int) {
	//defer func(){
	//	wg.Done()
	//}()

	ok, joinResult := jw.getNewJoinResult(ctx)
	if !ok {
		return
	}

	// get first inner chunk
	if !jw.fetchNextInnerChunk(ctx) {
		return
	}

	// get first rowsWithSameKey of inner chunk
	rowsWithSameKey, err := jw.getRowsWithSameKey()

	if err != nil {
		return
	}

	var s int
	var mt *mergeTask
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

		if len(mt.outerRows) == 2 {
			fmt.Println("first task:", i)
			trap = false
		}

		if len(mt.outerRows) == 3 {
			fmt.Println("second task:", i)
			trap = false
		}

		if len(mt.outerRows) == 1 {
			fmt.Println("third task:", i)
			trap = false
		}

		if len(mt.outerRows) == 4 {
			fmt.Println("fourth task:", i)
			trap = false
		}

		// 2.use for{} to get the sameKeyGroup rows from inner table chunk and compare with outer table rows in merge task
		for {
			cmpResult := -1

			cmpResult = compareIOChunkRow(jw.compareFuncs, mt.outerRows[0], rowsWithSameKey[0], jw.outerJoinKeys, jw.innerJoinKeys)

			if cmpResult > 0 {
				rowsWithSameKey, err = jw.getRowsWithSameKey()
				if err != nil {
					return
				}
				if len(rowsWithSameKey) == 0 {
					// 1.send doneCh to inner worker
					jw.doneCh <- true
					// 2.then get the next inner chunk
					//ok := jw.fetchNextInnerChunk(ctx)
					//if !ok || jw.innerChunk.NumRows() == 0 {
					//	return
					//}
					innerResult, ok := <-jw.innerFetchResultCh
					if ok {
						jw.innerChunk = innerResult.fetchChunk
						jw.innerIter4Row = chunk.NewIterator4Chunk(jw.innerChunk)
						jw.firstRow4Key = jw.innerIter4Row.Begin()
						jw.curInnerRow = jw.innerIter4Row.Next()
					}
					if innerResult == nil {
						mt.joinResultCh <- joinResult
						return
					}
					trap = true
					rowsWithSameKey, err = jw.getRowsWithSameKey()
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
					if len(jw.innerCache) > 0 {
						cmpCache := -1
						cmpCache = compareIOChunkRow(jw.compareFuncs, mt.outerRows[0], jw.innerCache[0], jw.outerJoinKeys, jw.innerJoinKeys)
						if cmpCache == 0 {
							// do join with rowsWithSameKey
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
				if mt.lastTaskFlag || jw.curInnerRow == jw.innerIter4Row.End() {
					if jw.curInnerRow == jw.innerIter4Row.End() {
						// the firstRow4Key == rowsWithSameKey's key, then there is no next rowsWithSameKey in this chunk
						// otherwise there may have final firstRow4Key as rowsWithSameKey
						if jw.firstRow4Key == jw.innerIter4Row.End() {
							// this chunk has no more row, then get next chunk
							jw.doneCh <- true
							//ok := jw.fetchNextInnerChunk(ctx)
							//if !ok {
							//	return
							//}
							innerResult, ok := <-jw.innerFetchResultCh
							if ok {
								jw.innerChunk = innerResult.fetchChunk
								jw.innerIter4Row = chunk.NewIterator4Chunk(jw.innerChunk)
								jw.firstRow4Key = jw.innerIter4Row.Begin()
								jw.curInnerRow = jw.innerIter4Row.Next()
							}
							if innerResult == nil {
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
									}
								}
							} else {
								for _, row := range rowsWithSameKey {
									jw.innerCache = append(jw.innerCache, row)
								}
								trap = true
								rowsWithSameKey, err = jw.getRowsWithSameKey()
								if err != nil {
									return
								}
								continue
							}
						} else {
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
								}
							}
						}
					} else {
						// the mergeTask is the last task in this chunk
						for _, row := range rowsWithSameKey {
							jw.innerCache = append(jw.innerCache, row)
						}
						// get the next chunk's first merge task
						break
					}
				} else {
					//var joinRows []chunk.Row
					var innerIter4Row chunk.Iterator
					if mt.firstTaskFlag {
						if len(jw.innerCache) > 0 {
							cmpCache := -1
							cmpCache = compareIOChunkRow(jw.compareFuncs, mt.outerRows[0], jw.innerCache[0], jw.outerJoinKeys, jw.innerJoinKeys)
							if cmpCache == 0 {
								innerIter4Row = chunk.NewIterator4Slice(jw.innerCache)
								innerIter4Row.Begin()
							} else {
								jw.innerCache = jw.innerCache[:0]
							}
						}
					} else {
						innerIter4Row = chunk.NewIterator4Slice(rowsWithSameKey)
						innerIter4Row.Begin()
					}

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
				close(mt.joinResultCh)
				break
			} else {
				close(mt.joinResultCh)
				break
			}
		}

	}
}

func (jw *mergeJoinWorker) fetchNextInnerChunk(ctx context.Context) bool {
	select {
	case innerResult, ok := <-jw.innerFetchResultCh:
		if !ok || innerResult.err != nil {
			return true
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

func (jw *mergeJoinWorker) getRowsWithSameKey() ([]chunk.Row, error) {
	// no more data.
	if jw.firstRow4Key == jw.innerIter4Row.End() {
		return nil, nil
	}
	var sameKeyRows []chunk.Row
	//t.sameKeyRows = t.sameKeyRows[:0]
	//t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	sameKeyRows = append(sameKeyRows, jw.firstRow4Key)
	for {
		selectedRow, err := jw.nextInnerChunkRow()
		// error happens or no more data.
		if err != nil || selectedRow == jw.innerIter4Row.End() {
			jw.firstRow4Key = jw.innerIter4Row.End()
			//return t.sameKeyRows, err
			//sameKeyRows = append(sameKeyRows, )
			return sameKeyRows, err
		}
		compareResult := compareIOChunkRow(jw.compareFuncs, selectedRow, jw.firstRow4Key, jw.innerJoinKeys, jw.innerJoinKeys)
		if compareResult == 0 {
			//t.sameKeyRows = append(t.sameKeyRows, selectedRow)
			sameKeyRows = append(sameKeyRows, selectedRow)
		} else {
			jw.firstRow4Key = selectedRow
			//return t.sameKeyRows, nil
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

func compareIOChunkRow(cmpFuncs []chunk.CompareFunc, lhsRow, rhsRow chunk.Row, lhsKey, rhsKey []*expression.Column) int {
	for i := range lhsKey {
		cmp := cmpFuncs[i](lhsRow, lhsKey[i].Index, rhsRow, rhsKey[i].Index)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	//e.memTracker.Detach()
	//e.memTracker = nil

	close(e.closeCh)

	for _, joinChkResourceCh := range e.joinChkResourceChs {
		close(joinChkResourceCh)
		for range joinChkResourceCh {
		}
	}
	return errors.Trace(e.baseExecutor.Close())
}
