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
	"encoding/binary"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// calcFraction is used to calculate the fraction of the interval [lower, upper] that lies within the [lower, value]
// using the continuous-value assumption.
func calcFraction(lower, upper, value float64) float64 {
	if upper <= lower {
		return 0.5
	}
	if value <= lower {
		return 0
	}
	if value >= upper {
		return 1
	}
	frac := (value - lower) / (upper - lower)
	if math.IsNaN(frac) || math.IsInf(frac, 0) || frac < 0 || frac > 1 {
		return 0.5
	}
	return frac
}

func calcRangeFraction(left, right, lInner, rInner float64) float64 {
	if left == right {
		if lInner == rInner && lInner == left {
			return 1
		}
		return 0
	}
	return (rInner - lInner) / (right - left)
}

func convertDatumToScalar(value *types.Datum, commonPfxLen int) float64 {
	switch value.Kind() {
	case types.KindMysqlDecimal:
		scalar, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return 0
		}
		return scalar
	case types.KindMysqlTime:
		valueTime := value.GetMysqlTime()
		return convertTimeToScalar(&valueTime)
	case types.KindString, types.KindBytes:
		bytes := value.GetBytes()
		if len(bytes) <= commonPfxLen {
			return 0
		}
		return convertBytesToScalar(bytes[commonPfxLen:])
	default:
		// do not know how to convert
		return 0
	}
}

func convertTimeToScalar(t *types.Time) float64 {
	var minTime types.Time
	switch t.Type {
	case mysql.TypeDate:
		minTime = types.Time{
			Time: types.MinDatetime,
			Type: mysql.TypeDate,
			Fsp:  types.DefaultFsp,
		}
	case mysql.TypeDatetime:
		minTime = types.Time{
			Time: types.MinDatetime,
			Type: mysql.TypeDatetime,
			Fsp:  types.DefaultFsp,
		}
	case mysql.TypeTimestamp:
		minTime = types.MinTimestamp
	}
	sc := &stmtctx.StatementContext{TimeZone: types.BoundTimezone}
	return float64(t.Sub(sc, &minTime).Duration)
}

// PreCalculateScalar converts the lower and upper to scalar. When the datum type is KindString or KindBytes, we also
// calculate their common prefix length, because when a value falls between lower and upper, the common prefix
// of lower and upper equals to the common prefix of the lower, upper and the value. For some simple types like `Int64`,
// we do not convert it because we can directly infer the scalar value.
func (hg *Histogram) PreCalculateScalar() {
	len := hg.Len()
	if len == 0 {
		return
	}
	switch hg.GetLower(0).Kind() {
	case types.KindMysqlDecimal, types.KindMysqlTime:
		hg.scalars = make([]scalar, len)
		for i := 0; i < len; i++ {
			hg.scalars[i] = scalar{
				lower: convertDatumToScalar(hg.GetLower(i), 0),
				upper: convertDatumToScalar(hg.GetUpper(i), 0),
			}
		}
	case types.KindBytes, types.KindString:
		hg.scalars = make([]scalar, len)
		for i := 0; i < len; i++ {
			lower, upper := hg.GetLower(i), hg.GetUpper(i)
			common := commonPrefixLength(lower.GetBytes(), upper.GetBytes())
			hg.scalars[i] = scalar{
				commonPfxLen: common,
				lower:        convertDatumToScalar(lower, common),
				upper:        convertDatumToScalar(upper, common),
			}
		}
	}
}

func (hg *Histogram) calcFraction(index int, value *types.Datum) float64 {
	lower, upper := hg.Bounds.GetRow(2*index), hg.Bounds.GetRow(2*index+1)
	switch value.Kind() {
	case types.KindFloat32:
		return calcFraction(float64(lower.GetFloat32(0)), float64(upper.GetFloat32(0)), float64(value.GetFloat32()))
	case types.KindFloat64:
		return calcFraction(lower.GetFloat64(0), upper.GetFloat64(0), value.GetFloat64())
	case types.KindInt64:
		return calcFraction(float64(lower.GetInt64(0)), float64(upper.GetInt64(0)), float64(value.GetInt64()))
	case types.KindUint64:
		return calcFraction(float64(lower.GetUint64(0)), float64(upper.GetUint64(0)), float64(value.GetUint64()))
	case types.KindMysqlDuration:
		return calcFraction(float64(lower.GetDuration(0, 0).Duration), float64(upper.GetDuration(0, 0).Duration), float64(value.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal, types.KindMysqlTime:
		return calcFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertDatumToScalar(value, 0))
	case types.KindBytes, types.KindString:
		return calcFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertDatumToScalar(value, hg.scalars[index].commonPfxLen))
	}
	return 0.5
}

func (hg *Histogram) calcRangeFraction(index int, lInner, rInner chunk.Row) float64 {
	left, right := hg.Bounds.GetRow(2*index), hg.Bounds.GetRow(2*index+1)
	switch hg.Tp.Tp {
	// TODO: support more types.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(hg.Tp.Flag) {
			return calcRangeFraction(float64(left.GetUint64(0)), float64(right.GetUint64(0))+1, float64(lInner.GetUint64(0)), float64(rInner.GetUint64(0))+1)
		}
		return calcRangeFraction(float64(left.GetInt64(0)), float64(right.GetInt64(0))+1, float64(lInner.GetInt64(0)), float64(rInner.GetInt64(0))+1)
	case mysql.TypeFloat:
		return calcRangeFraction(float64(left.GetFloat32(0)), float64(right.GetFloat32(0)), float64(lInner.GetFloat32(0)), float64(rInner.GetFloat32(0)))
	case mysql.TypeDouble:
		return calcRangeFraction(left.GetFloat64(0), right.GetFloat64(0), lInner.GetFloat64(0), rInner.GetFloat64(0))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		commonLen := commonPrefixLength(left.GetBytes(0), right.GetBytes(0))
		return calcRangeFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertBytesToScalar(lInner.GetBytes(0)[commonLen:]), convertBytesToScalar(rInner.GetBytes(0)[commonLen:]))
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		lTime := lInner.GetTime(0)
		rTime := rInner.GetTime(0)
		return calcRangeFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertTimeToScalar(&lTime), convertTimeToScalar(&rTime))
	case mysql.TypeNewDecimal:
		lDec, err := lInner.GetMyDecimal(0).ToFloat64()
		if err != nil {
			return 0
		}
		rDec, err := rInner.GetMyDecimal(0).ToFloat64()
		if err != nil {
			return 0
		}
		return calcRangeFraction(hg.scalars[index].lower, hg.scalars[index].upper, lDec, rDec)
	case mysql.TypeDuration:
		return calcRangeFraction(float64(left.GetDuration(0, 0).Duration), float64(right.GetDuration(0, 0).Duration), float64(lInner.GetDuration(0, 0).Duration), float64(rInner.GetDuration(0, 0).Duration))
	}
	return 0.5
}

func commonPrefixLength(lower, upper []byte) int {
	minLen := len(lower)
	if minLen > len(upper) {
		minLen = len(upper)
	}
	for i := 0; i < minLen; i++ {
		if lower[i] != upper[i] {
			return i
		}
	}
	return minLen
}

func convertBytesToScalar(value []byte) float64 {
	// Bytes type is viewed as a base-256 value, so we only consider at most 8 bytes.
	var buf [8]byte
	copy(buf[:], value)
	return float64(binary.BigEndian.Uint64(buf[:]))
}

func calcFraction4Datums(lower, upper, value *types.Datum) float64 {
	switch value.Kind() {
	case types.KindFloat32:
		return calcFraction(float64(lower.GetFloat32()), float64(upper.GetFloat32()), float64(value.GetFloat32()))
	case types.KindFloat64:
		return calcFraction(lower.GetFloat64(), upper.GetFloat64(), value.GetFloat64())
	case types.KindInt64:
		return calcFraction(float64(lower.GetInt64()), float64(upper.GetInt64()), float64(value.GetInt64()))
	case types.KindUint64:
		return calcFraction(float64(lower.GetUint64()), float64(upper.GetUint64()), float64(value.GetUint64()))
	case types.KindMysqlDuration:
		return calcFraction(float64(lower.GetMysqlDuration().Duration), float64(upper.GetMysqlDuration().Duration), float64(value.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal, types.KindMysqlTime:
		return calcFraction(convertDatumToScalar(lower, 0), convertDatumToScalar(upper, 0), convertDatumToScalar(value, 0))
	case types.KindBytes, types.KindString:
		commonPfxLen := commonPrefixLength(lower.GetBytes(), upper.GetBytes())
		return calcFraction(convertDatumToScalar(lower, commonPfxLen), convertDatumToScalar(upper, commonPfxLen), convertDatumToScalar(value, commonPfxLen))
	}
	return 0.5
}
