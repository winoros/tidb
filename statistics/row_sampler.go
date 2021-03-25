package statistics

import (
	"bytes"
	"container/heap"
	"context"
	"math/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

type RowSampleCollector struct {
	Samples       WeightedRowSampleHeap
	NullCount     []int64
	FMSketches    []*FMSketch
	TotalSizes    []int64
	Count         int64
	MaxSampleSize int
}

type RowSampleItem struct {
	Columns []types.Datum
	Weight  int64
}

type WeightedRowSampleHeap []*RowSampleItem

func (h WeightedRowSampleHeap) Len() int {
	return len(h)
}

func (h WeightedRowSampleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h WeightedRowSampleHeap) Less(i, j int) bool {
	return h[i].Weight < h[j].Weight
}

func (h *WeightedRowSampleHeap) Push(i interface{}) {
	*h = append(*h, i.(*RowSampleItem))
}

func (h *WeightedRowSampleHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type RowSampleBuilder struct {
	Sc              *stmtctx.StatementContext
	RecordSet       sqlexec.RecordSet
	ColsFieldType   []*types.FieldType
	Collators       []collate.Collator
	ColGroups       [][]int64
	MaxSampleSize   int
	MaxFMSketchSize int
	Rng             *rand.Rand
}

func (s *RowSampleBuilder) Collect() (*RowSampleCollector, error) {
	collector := &RowSampleCollector{
		Samples:       make(WeightedRowSampleHeap, 0, s.MaxSampleSize),
		NullCount:     make([]int64, len(s.ColsFieldType)+len(s.ColGroups)),
		FMSketches:    make([]*FMSketch, 0, len(s.ColsFieldType)+len(s.ColGroups)),
		TotalSizes:    make([]int64, len(s.ColsFieldType)+len(s.ColGroups)),
		MaxSampleSize: s.MaxSampleSize,
	}
	for i := 0; i < len(s.ColsFieldType)+len(s.ColGroups); i++ {
		collector.FMSketches = append(collector.FMSketches, NewFMSketch(int(s.MaxFMSketchSize)))
	}
	ctx := context.TODO()
	chk := s.RecordSet.NewChunk()
	it := chunk.NewIterator4Chunk(chk)
	for {
		err := s.RecordSet.Next(ctx, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			return collector, nil
		}
		collector.Count += int64(chk.NumRows())
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, s.RecordSet.Fields())
			for i, val := range datums {
				if s.Collators[i] != nil && !val.IsNull() {
					decodedVal, err := tablecodec.DecodeColumnValue(val.GetBytes(), s.ColsFieldType[i], s.Sc.TimeZone)
					if err != nil {
						return nil, err
					}
					decodedVal.SetBytesAsString(s.Collators[i].Key(decodedVal.GetString()), decodedVal.Collation(), uint32(decodedVal.Length()))
					encodedKey, err := tablecodec.EncodeValue(s.Sc, nil, decodedVal)
					if err != nil {
						return nil, err
					}
					val.SetBytes(encodedKey)
				}
				err := collector.collectColumns(s.Sc, datums)
				if err != nil {
					return nil, err
				}
				err = collector.collectIndexes(s.Sc, datums, s.ColGroups)
				if err != nil {
					return nil, err
				}
			}
			weight := s.Rng.Int63()
			collector.sampleRow(datums, weight)
		}
	}
}

func (s *RowSampleCollector) collectColumns(sc *stmtctx.StatementContext, cols []types.Datum) error {
	for i, col := range cols {
		if col.IsNull() {
			s.NullCount[i]++
		}
		s.TotalSizes[i] += int64(len(col.GetBytes())) - 1
		// Minus one is to remove the flag byte.
		err := s.FMSketches[i].InsertValue(sc, col)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *RowSampleCollector) collectIndexes(sc *stmtctx.StatementContext, cols []types.Datum, colGroups [][]int64) error {
	colLen := len(cols)
	datumBuffer := make([]types.Datum, 0, len(cols))
	for i, group := range colGroups {
		datumBuffer = datumBuffer[:0]
		allNull := true
		for _, c := range group {
			datumBuffer = append(datumBuffer, cols[c])
			allNull = allNull && cols[c].IsNull()
			s.TotalSizes[colLen+i] += int64(len(cols[c].GetBytes())) - 1
		}
		err := s.FMSketches[colLen+i].InsertRowValue(sc, datumBuffer)
		if err != nil {
			return err
		}
		if allNull {
			s.NullCount[colLen+i]++
		}
	}
	return nil
}

func (s *RowSampleCollector) sampleRow(cols []types.Datum, weight int64) {
	if len(s.Samples) < s.MaxSampleSize {
		newCols := make([]types.Datum, len(cols))
		for i := range cols {
			cols[i].Copy(&newCols[i])
		}
		s.Samples = append(s.Samples, &RowSampleItem{
			Columns: newCols,
			Weight:  weight,
		})
		if len(s.Samples) == s.MaxSampleSize {
			heap.Init(&s.Samples)
		}
		return
	}
	if s.Samples[0].Weight < weight {
		for i := range cols {
			cols[i].Copy(&s.Samples[0].Columns[i])
		}
		s.Samples[0].Weight = weight
		heap.Fix(&s.Samples, 0)
	}
}

func (s *RowSampleCollector) sampleZippedRow(sample *RowSampleItem) {
	if len(s.Samples) < s.MaxSampleSize {
		s.Samples = append(s.Samples, sample)
		if len(s.Samples) == s.MaxSampleSize {
			heap.Init(&s.Samples)
		}
		return
	}
	if s.Samples[0].Weight < sample.Weight {
		s.Samples[0] = sample
		heap.Fix(&s.Samples, 0)
	}
}

func (s *RowSampleCollector) ToProto() *tipb.RowSampleCollector {
	pbFMSketches := make([]*tipb.FMSketch, 0, len(s.FMSketches))
	for _, sketch := range s.FMSketches {
		pbFMSketches = append(pbFMSketches, FMSketchToProto(sketch))
	}
	collector := &tipb.RowSampleCollector{
		Samples:    RowSamplesToProto(s.Samples),
		NullCounts: s.NullCount,
		Count:      s.Count,
		FmSketch:   pbFMSketches,
		TotalSize:  s.TotalSizes,
	}
	return collector
}

func (s *RowSampleCollector) FromProto(pbCollector *tipb.RowSampleCollector) {
	s.Count = pbCollector.Count
	s.NullCount = pbCollector.NullCounts
	s.FMSketches = make([]*FMSketch, 0, len(pbCollector.FmSketch))
	for _, pbSketch := range pbCollector.FmSketch {
		s.FMSketches = append(s.FMSketches, FMSketchFromProto(pbSketch))
	}
	s.TotalSizes = pbCollector.TotalSize
	s.Samples = make(WeightedRowSampleHeap, 0, len(pbCollector.Samples))
	for _, pbSample := range pbCollector.Samples {
		data := make([]types.Datum, 0, len(pbSample.Row))
		for _, col := range pbSample.Row {
			b := make([]byte, len(col))
			copy(b, col)
			data = append(data, types.NewBytesDatum(b))
		}
		s.Samples = append(s.Samples, &RowSampleItem{
			Columns: data,
			Weight:  pbSample.Weight,
		})
	}
	heap.Init(&s.Samples)
}

func (s *RowSampleCollector) MergeCollector(subCollector *RowSampleCollector) {
	s.Count += subCollector.Count
	for i := range subCollector.FMSketches {
		s.FMSketches[i].MergeFMSketch(subCollector.FMSketches[i])
	}
	for i := range subCollector.NullCount {
		s.NullCount[i] += subCollector.NullCount[i]
	}
	for i := range subCollector.TotalSizes {
		s.TotalSizes[i] += subCollector.TotalSizes[i]
	}
	for _, sample := range subCollector.Samples {
		s.sampleZippedRow(sample)
	}
}

func RowSamplesToProto(samples WeightedRowSampleHeap) []*tipb.RowSample {
	if len(samples) == 0 {
		return nil
	}
	rows := make([]*tipb.RowSample, 0, len(samples))
	colLen := len(samples[0].Columns)
	for _, sample := range samples {
		pbRow := &tipb.RowSample{
			Row:    make([][]byte, 0, colLen),
			Weight: sample.Weight,
		}
		for _, c := range sample.Columns {
			pbRow.Row = append(pbRow.Row, c.GetBytes())
		}
		rows = append(rows, pbRow)
	}
	return rows
}

// BuildColumnHistAndTopNOnRowSample build a histogram and TopN for a column from samples.
func BuildColumnHistAndTopNOnRowSample(
	ctx sessionctx.Context,
	numBuckets, numTopN int,
	id int64,
	collector *SampleCollector,
	tp *types.FieldType,
) (*Histogram, *TopN, error) {
	count := collector.Count
	ndv := collector.FMSketch.NDV()
	nullCount := collector.NullCount
	if ndv > count {
		ndv = count
	}
	if count == 0 || len(collector.Samples) == 0 {
		return NewHistogram(id, ndv, nullCount, 0, tp, 0, collector.TotalSize), nil, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	samples := collector.Samples
	samples, err := SortSampleItems(sc, samples)
	if err != nil {
		return nil, nil, err
	}
	hg := NewHistogram(id, ndv, nullCount, 0, tp, numBuckets, collector.TotalSize)

	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(samples))

	// Step1: collect topn from samples

	// the topNList is always sorted by count from more to less
	topNList := make([]TopNMeta, 0, numTopN)
	cur, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx, nil, samples[0].Value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	curCnt := float64(0)

	// Iterate through the samples
	for i := int64(0); i < sampleNum; i++ {

		sampleBytes, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx, nil, samples[i].Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// case 1, this value is equal to the last one: current count++
		if bytes.Equal(cur, sampleBytes) {
			curCnt += 1
			continue
		}
		// case 2, meet a different value: counting for the "current" is complete
		// case 2-1, now topn is empty: append the "current" count directly
		if len(topNList) == 0 {
			topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-2, now topn is full, and the "current" count is less than the least count in the topn: no need to insert the "current"
		if len(topNList) >= numTopN && uint64(curCnt) <= topNList[len(topNList)-1].Count {
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-3, now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
		j := len(topNList)
		for ; j > 0; j-- {
			if uint64(curCnt) < topNList[j-1].Count {
				break
			}
		}
		topNList = append(topNList, TopNMeta{})
		copy(topNList[j+1:], topNList[j:])
		topNList[j] = TopNMeta{Encoded: cur, Count: uint64(curCnt)}
		if len(topNList) > numTopN {
			topNList = topNList[:numTopN]
		}
		cur, curCnt = sampleBytes, 1
	}

	// Handle the counting for the last value. Basically equal to the case 2 above.
	// now topn is empty: append the "current" count directly
	if len(topNList) == 0 {
		topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
	} else if len(topNList) < numTopN || uint64(curCnt) > topNList[len(topNList)-1].Count {
		// now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
		j := len(topNList)
		for ; j > 0; j-- {
			if uint64(curCnt) < topNList[j-1].Count {
				break
			}
		}
		topNList = append(topNList, TopNMeta{})
		copy(topNList[j+1:], topNList[j:])
		topNList[j] = TopNMeta{Encoded: cur, Count: uint64(curCnt)}
		if len(topNList) > numTopN {
			topNList = topNList[:numTopN]
		}
	}

	// Step2: exclude topn from samples
	for i := int64(0); i < int64(len(samples)); i++ {
		sampleBytes, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx, nil, samples[i].Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for j := 0; j < len(topNList); j++ {
			if bytes.Equal(sampleBytes, topNList[j].Encoded) {
				// find the same value in topn: need to skip over this value in samples
				copy(samples[i:], samples[uint64(i)+topNList[j].Count:])
				samples = samples[:uint64(len(samples))-topNList[j].Count]
				i--
				continue
			}
		}
	}

	for i := 0; i < len(topNList); i++ {
		topNList[i].Count *= uint64(sampleFactor)
	}
	topn := &TopN{TopN: topNList}

	if uint64(count) <= topn.TotalCount() || int(hg.NDV) <= len(topn.TopN) {
		// TopN includes all sample data
		return hg, topn, nil
	}

	// Step3: build histogram with the rest samples
	if len(samples) > 0 {
		_, err = buildHist(sc, hg, samples, count-int64(topn.TotalCount()), ndv-int64(len(topn.TopN)), int64(numBuckets))
		if err != nil {
			return nil, nil, err
		}
	}

	return hg, topn, nil
}

// BuildIdxHistAndTopNOnRowSample build a histogram and TopN for a column from samples.
func BuildIdxHistAndTopNOnRowSample(
	ctx sessionctx.Context,
	numBuckets, numTopN int,
	id int64,
	collector *SampleCollector,
	tp *types.FieldType,
) (*Histogram, *TopN, error) {
	count := collector.Count
	ndv := collector.FMSketch.NDV()
	nullCount := collector.NullCount
	if ndv > count {
		ndv = count
	}
	if count == 0 || len(collector.Samples) == 0 {
		return NewHistogram(id, ndv, nullCount, 0, tp, 0, collector.TotalSize), nil, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	samples := collector.Samples
	samples, err := SortSampleItems(sc, samples)
	if err != nil {
		return nil, nil, err
	}
	hg := NewHistogram(id, ndv, nullCount, 0, tp, numBuckets, collector.TotalSize)

	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(samples))

	// Step1: collect topn from samples

	// the topNList is always sorted by count from more to less
	topNList := make([]TopNMeta, 0, numTopN)
	cur := samples[0].Value.GetBytes()
	curCnt := float64(0)

	// Iterate through the samples
	for i := int64(0); i < sampleNum; i++ {

		sampleBytes := samples[i].Value.GetBytes()
		// case 1, this value is equal to the last one: current count++
		if bytes.Equal(cur, sampleBytes) {
			curCnt += 1
			continue
		}
		// case 2, meet a different value: counting for the "current" is complete
		// case 2-1, now topn is empty: append the "current" count directly
		if len(topNList) == 0 {
			topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-2, now topn is full, and the "current" count is less than the least count in the topn: no need to insert the "current"
		if len(topNList) >= numTopN && uint64(curCnt) <= topNList[len(topNList)-1].Count {
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-3, now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
		j := len(topNList)
		for ; j > 0; j-- {
			if uint64(curCnt) < topNList[j-1].Count {
				break
			}
		}
		topNList = append(topNList, TopNMeta{})
		copy(topNList[j+1:], topNList[j:])
		topNList[j] = TopNMeta{Encoded: cur, Count: uint64(curCnt)}
		if len(topNList) > numTopN {
			topNList = topNList[:numTopN]
		}
		cur, curCnt = sampleBytes, 1
	}

	// Handle the counting for the last value. Basically equal to the case 2 above.
	// now topn is empty: append the "current" count directly
	if len(topNList) == 0 {
		topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
	} else if len(topNList) < numTopN || uint64(curCnt) > topNList[len(topNList)-1].Count {
		// now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
		j := len(topNList)
		for ; j > 0; j-- {
			if uint64(curCnt) < topNList[j-1].Count {
				break
			}
		}
		topNList = append(topNList, TopNMeta{})
		copy(topNList[j+1:], topNList[j:])
		topNList[j] = TopNMeta{Encoded: cur, Count: uint64(curCnt)}
		if len(topNList) > numTopN {
			topNList = topNList[:numTopN]
		}
	}

	// Step2: exclude topn from samples
	for i := int64(0); i < int64(len(samples)); i++ {
		sampleBytes := samples[i].Value.GetBytes()
		for j := 0; j < len(topNList); j++ {
			if bytes.Equal(sampleBytes, topNList[j].Encoded) {
				// find the same value in topn: need to skip over this value in samples
				copy(samples[i:], samples[uint64(i)+topNList[j].Count:])
				samples = samples[:uint64(len(samples))-topNList[j].Count]
				i--
				continue
			}
		}
	}

	for i := 0; i < len(topNList); i++ {
		topNList[i].Count *= uint64(sampleFactor)
	}
	topn := &TopN{TopN: topNList}

	if uint64(count) <= topn.TotalCount() || int(hg.NDV) <= len(topn.TopN) {
		// TopN includes all sample data
		return hg, topn, nil
	}

	// Step3: build histogram with the rest samples
	if len(samples) > 0 {
		_, err = buildHist(sc, hg, samples, count-int64(topn.TotalCount()), ndv-int64(len(topn.TopN)), int64(numBuckets))
		if err != nil {
			return nil, nil, err
		}
	}

	return hg, topn, nil
}
