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

package statistics

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
)

var _ = Suite(&testSampleSuite{})

type testSampleSuite struct {
	count int
	rs    sqlexec.RecordSet
}

func (s *testSampleSuite) SetUpSuite(c *C) {
	s.count = 10000
	rs := &recordSet{
		data:      make([]types.Datum, s.count),
		count:     s.count,
		cursor:    0,
		firstIsID: true,
	}
	rs.setFields(mysql.TypeLonglong, mysql.TypeLonglong)
	start := 1000 // 1000 values is null
	for i := start; i < rs.count; i++ {
		rs.data[i].SetInt64(int64(i))
	}
	for i := start; i < rs.count; i += 3 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 1)
	}
	for i := start; i < rs.count; i += 5 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 2)
	}
	s.rs = rs
}

func (s *testSampleSuite) TestCollectColumnStats(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	builder := SampleBuilder{
		Sc:              sc,
		RecordSet:       s.rs,
		ColLen:          1,
		PkBuilder:       NewSortedBuilder(sc, 256, 1, types.NewFieldType(mysql.TypeLonglong), Version2),
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		Collators:       make([]collate.Collator, 1),
		ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(collectors[0].NullCount+collectors[0].Count, Equals, int64(s.count))
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(6232))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
	c.Assert(pkBuilder.Count, Equals, int64(s.count))
	c.Assert(pkBuilder.Hist().NDV, Equals, int64(s.count))
}

func (s *testSampleSuite) TestMergeSampleCollector(c *C) {
	builder := SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          2,
		MaxSampleSize:   1000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		Collators:       make([]collate.Collator, 2),
		ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	c.Assert(len(collectors), Equals, 2)
	collectors[0].IsMerger = true
	collectors[0].MergeSampleCollector(sc, collectors[1])
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(9280))
	c.Assert(len(collectors[0].Samples), Equals, 1000)
	c.Assert(collectors[0].NullCount, Equals, int64(1000))
	c.Assert(collectors[0].Count, Equals, int64(19000))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
}

func (s *testSampleSuite) TestCollectorProtoConversion(c *C) {
	builder := SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          2,
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		Collators:       make([]collate.Collator, 2),
		ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	for _, collector := range collectors {
		p := SampleCollectorToProto(collector)
		s := SampleCollectorFromProto(p)
		c.Assert(collector.Count, Equals, s.Count)
		c.Assert(collector.NullCount, Equals, s.NullCount)
		c.Assert(collector.CMSketch.TotalCount(), Equals, s.CMSketch.TotalCount())
		c.Assert(collector.FMSketch.NDV(), Equals, s.FMSketch.NDV())
		c.Assert(collector.TotalSize, Equals, s.TotalSize)
		c.Assert(len(collector.Samples), Equals, len(s.Samples))
	}
}

func (s *testSampleSuite) TestWeightedSampling(c *C) {
	for x := 0; x < 800; x++ {
		sampleNum := int64(20)
		rowNum := 100
		loopCnt := 500
		collector := SampleCollector{
			Samples:       make(weightedItemHeap, 0, 20),
			MaxSampleSize: sampleNum,
		}
		origItems := make([]types.Datum, 0, 100)
		for i := 0; i < rowNum; i++ {
			origItems = append(origItems, types.NewIntDatum(int64(i)))
		}
		itemCnt := make([]int, rowNum)
		for loopI := 0; loopI < loopCnt; loopI++ {
			collector.Samples = collector.Samples[:0]
			for i := 0; i < rowNum; i++ {
				err := collector.doWeightedSample(&origItems[i])
				c.Assert(err, IsNil)
			}
			for i := 0; i < int(collector.MaxSampleSize); i++ {
				itemCnt[collector.Samples[i].Value.GetInt64()]++
			}
		}
		expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
		delta := 0.5
		for _, cnt := range itemCnt {
			if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
				c.Assert(false, IsTrue, Commentf("Round %v, the frequency %v is exceed the Chernoff Bound", x, cnt))
			}
		}
	}
}

func (s *testSampleSuite) TestDistributedWeightedSampling(c *C) {
	for x := 0; x < 800; x++ {
		sampleNum := int64(10)
		rowNum := 100
		loopCnt := 1000
		rootCollector := SampleCollector{
			Samples:       make(weightedItemHeap, 0, sampleNum),
			MaxSampleSize: sampleNum,
		}
		regionCollector := make([]SampleCollector, 0, 5)
		for i := 0; i < 5; i++ {
			regionCollector = append(regionCollector, SampleCollector{
				Samples:       make(weightedItemHeap, 0, sampleNum),
				MaxSampleSize: sampleNum,
			})
		}
		origItem := make([]types.Datum, 0, rowNum)
		for i := 0; i < rowNum; i++ {
			origItem = append(origItem, types.NewIntDatum(int64(i)))
		}
		itemCnt := make([]int, rowNum)
		for loopI := 1; loopI < loopCnt; loopI++ {
			rootCollector.Samples = rootCollector.Samples[:0]
			for i := 0; i < 5; i++ {
				regionCollector[i].Samples = regionCollector[i].Samples[:0]
				for j := 0; j < 20; j++ {
					regionCollector[i].doWeightedSample(&origItem[i*20+j])
				}
				rootCollector.dowWeightedSamplingFromSubCollector(&regionCollector[i])
			}
			for i := 0; i < int(rootCollector.MaxSampleSize); i++ {
				itemCnt[rootCollector.Samples[i].Value.GetInt64()]++
			}
		}
		expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
		delta := 0.5
		for _, cnt := range itemCnt {
			if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
				c.Assert(false, IsTrue, Commentf("In round %v, the frequency %v is exceed the Chernoff Bound", x, cnt))
			}
		}
	}
}

// The following codes are testing the Reservoir Sampling of TiDB before 2021.
//type simpleSampleSet struct {
//	samples    []int
//	sampleSize int
//	seenCnt    int
//}
//
//func (s *simpleSampleSet) simpleReservoirSampling(v int) {
//	s.seenCnt++
//	if len(s.samples) < s.sampleSize {
//		s.samples = append(s.samples, v)
//	} else {
//		shouldAdd := fastrand.Uint64N(uint64(s.seenCnt)) < uint64(s.sampleSize)
//		if shouldAdd {
//			idx := int(fastrand.Uint32N(uint32(s.sampleSize)))
//			s.samples = append(s.samples[:idx], s.samples[idx+1:]...)
//			s.samples = append(s.samples, v)
//		}
//
//}
//
//func (s *testSampleSuite) TestOrigSampling(c *C) {
//	for x := 0; x < 800; x++ {
//		sampleNum := 10
//		rowNum := 100
//		loopCnt := 1000
//		rootCollector := simpleSampleSet{
//			samples:    make([]int, 0, sampleNum),
//			sampleSize: sampleNum,
//		}
//		regionCollector := make([]simpleSampleSet, 0, 5)
//		for i := 0; i < 5; i++ {
//			regionCollector = append(regionCollector, simpleSampleSet{
//				samples:    make([]int, 0, sampleNum),
//				sampleSize: sampleNum,
//			})
//		}
//		rows := make([]int, 0, rowNum)
//		for i := 0; i < rowNum; i++ {
//			rows = append(rows, i)
//		}
//		itemCnt := make([]int, rowNum)
//		for loopI := 0; loopI < loopCnt; loopI++ {
//			rootCollector.samples = rootCollector.samples[:0]
//			rootCollector.seenCnt = 0
//			for i := 0; i < 5; i++ {
//				regionCollector[i].samples = regionCollector[i].samples[:0]
//				regionCollector[i].seenCnt = 0
//				for j := 0; j < rowNum/5; j++ {
//					regionCollector[i].simpleReservoirSampling(rows[i*20+j])
//				}
//				for j := 0; j < regionCollector[i].sampleSize; j++ {
//					rootCollector.simpleReservoirSampling(regionCollector[i].samples[j])
//				}
//			}
//			for i := 0; i < sampleNum; i++ {
//				itemCnt[rootCollector.samples[i]]++
//			}
//		}
//		expFrequency := float64(sampleNum) * float64(loopCnt) / float64(rowNum)
//		delta := 0.5
//		for _, cnt := range itemCnt {
//			if float64(cnt) < expFrequency/(1+delta) || float64(cnt) > expFrequency*(1+delta) {
//				c.Assert(false, IsTrue, Commentf("In round %v, the frequency %v is exceed the Chernoff Bound", x, cnt))
//			}
//		}
//	}
//}
