// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import "github.com/pingcap/tidb/pkg/parser/ast"

// NullPropagation classifies how a function behaves when any of its inputs is NULL.
// This is the foundation of the structural null-rejection analysis.
type NullPropagation int

const (
	// NullPropPreserving means any NULL input produces a NULL output.
	// Examples: +, -, *, /, =, <, >, LIKE, most scalar functions.
	NullPropPreserving NullPropagation = iota

	// NullPropHiding means the function may produce a non-NULL output from NULL inputs.
	// Examples: COALESCE, IFNULL, <=>, IF, CASE, CONCAT_WS, GREATEST, LEAST.
	NullPropHiding

	// NullPropSpecial means the function has dedicated recursive rules in the
	// null-rejection algorithm (AND, OR, NOT, IN, IS NULL, IS TRUE, etc.).
	NullPropSpecial
)

// nullPropagationMap classifies every function that can appear as a
// ScalarFunction.FuncName in TiDB.
//
// The completeness test TestAllBuiltinFunctionsClassified ensures that
// every entry in the `funcs` registry (plus the special-cased names)
// has an entry here. When a new builtin is added, that test fails,
// forcing the developer to classify the new function.
var nullPropagationMap = map[string]NullPropagation{
	// --- NullPropSpecial: handled by dedicated recursive rules ---
	ast.LogicAnd:           NullPropSpecial,
	ast.LogicOr:            NullPropSpecial,
	ast.UnaryNot:           NullPropSpecial,
	ast.In:                 NullPropSpecial,
	ast.IsNull:             NullPropSpecial,
	ast.IsTruthWithNull:    NullPropSpecial, // IS TRUE (with NULL → NULL)
	ast.IsTruthWithoutNull: NullPropSpecial, // IS TRUE (with NULL → FALSE)
	ast.IsFalsity:          NullPropSpecial, // IS FALSE

	// --- NullPropHiding: may produce non-NULL from NULL inputs ---
	ast.Coalesce:  NullPropHiding,
	ast.Ifnull:    NullPropHiding,
	ast.NullEQ:    NullPropHiding, // <=> operator
	ast.If:        NullPropHiding,
	ast.Case:      NullPropHiding,
	ast.Nullif:    NullPropHiding,
	ast.ConcatWS:  NullPropHiding, // skips NULL arguments
	ast.Greatest:  NullPropHiding,
	ast.Least:     NullPropHiding,
	ast.Interval:  NullPropHiding, // INTERVAL(N, N1, N2, ...) returns index
	ast.AnyValue:  NullPropHiding, // ANY_VALUE may return NULL but cannot be relied on
	ast.Elt:       NullPropHiding, // ELT(N, s1, s2, ...) may skip NULL
	ast.MakeSet:   NullPropHiding, // MAKE_SET(bits, s1, ...) skips NULLs
	ast.ExportSet: NullPropHiding, // has default values for NULL args
	ast.Field:     NullPropHiding, // FIELD(NULL, ...) returns 0, not NULL

	// --- NullPropPreserving: any NULL input → NULL output ---

	// Comparison operators
	ast.EQ:     NullPropPreserving,
	ast.NE:     NullPropPreserving,
	ast.LT:     NullPropPreserving,
	ast.LE:     NullPropPreserving,
	ast.GT:     NullPropPreserving,
	ast.GE:     NullPropPreserving,
	ast.Like:   NullPropPreserving,
	ast.Ilike:  NullPropPreserving,
	ast.Regexp: NullPropPreserving,

	// Arithmetic operators
	ast.Plus:       NullPropPreserving,
	ast.Minus:      NullPropPreserving,
	ast.Mul:        NullPropPreserving,
	ast.Div:        NullPropPreserving,
	ast.IntDiv:     NullPropPreserving,
	ast.Mod:        NullPropPreserving,
	ast.UnaryMinus: NullPropPreserving,

	// Bitwise operators
	ast.And:        NullPropPreserving, // bitwise AND
	ast.Or:         NullPropPreserving, // bitwise OR
	ast.Xor:        NullPropPreserving, // bitwise XOR
	ast.BitNeg:     NullPropPreserving,
	ast.LeftShift:  NullPropPreserving,
	ast.RightShift: NullPropPreserving,
	ast.BitCount:   NullPropPreserving,

	// Logic XOR (unlike AND/OR, XOR is strict: NULL XOR x = NULL)
	ast.LogicXor: NullPropPreserving,

	// Cast (special-cased in newFunctionImpl but still a ScalarFunction)
	ast.Cast: NullPropPreserving,

	// Math functions
	ast.Abs:      NullPropPreserving,
	ast.Acos:     NullPropPreserving,
	ast.Asin:     NullPropPreserving,
	ast.Atan:     NullPropPreserving,
	ast.Atan2:    NullPropPreserving,
	ast.Ceil:     NullPropPreserving,
	ast.Ceiling:  NullPropPreserving,
	ast.Conv:     NullPropPreserving,
	ast.Cos:      NullPropPreserving,
	ast.Cot:      NullPropPreserving,
	ast.CRC32:    NullPropPreserving,
	ast.Degrees:  NullPropPreserving,
	ast.Exp:      NullPropPreserving,
	ast.Floor:    NullPropPreserving,
	ast.Ln:       NullPropPreserving,
	ast.Log:      NullPropPreserving,
	ast.Log2:     NullPropPreserving,
	ast.Log10:    NullPropPreserving,
	ast.Pow:      NullPropPreserving,
	ast.Power:    NullPropPreserving,
	ast.Radians:  NullPropPreserving,
	ast.Rand:     NullPropPreserving,
	ast.Round:    NullPropPreserving,
	ast.Sign:     NullPropPreserving,
	ast.Sin:      NullPropPreserving,
	ast.Sqrt:     NullPropPreserving,
	ast.Tan:      NullPropPreserving,
	ast.Truncate: NullPropPreserving,

	// String functions (NULL in → NULL out)
	ast.ASCII:           NullPropPreserving,
	ast.Bin:             NullPropPreserving,
	ast.BitLength:       NullPropPreserving,
	ast.CharFunc:        NullPropPreserving,
	ast.CharLength:      NullPropPreserving,
	ast.CharacterLength: NullPropPreserving,
	ast.Concat:          NullPropPreserving, // CONCAT(NULL, ...) = NULL (unlike CONCAT_WS)
	ast.Convert:         NullPropPreserving,
	ast.Format:          NullPropPreserving,
	ast.FromBase64:      NullPropPreserving,
	ast.FindInSet:       NullPropPreserving,
	ast.Hex:             NullPropPreserving,
	ast.InsertFunc:      NullPropPreserving,
	ast.Instr:           NullPropPreserving,
	ast.Lcase:           NullPropPreserving,
	ast.Left:            NullPropPreserving,
	ast.Length:           NullPropPreserving,
	ast.LoadFile:        NullPropPreserving,
	ast.Locate:          NullPropPreserving,
	ast.Lower:           NullPropPreserving,
	ast.Lpad:            NullPropPreserving,
	ast.LTrim:           NullPropPreserving,
	ast.Mid:             NullPropPreserving,
	ast.Oct:             NullPropPreserving,
	ast.OctetLength:     NullPropPreserving,
	ast.Ord:             NullPropPreserving,
	ast.Position:        NullPropPreserving,
	ast.Quote:           NullPropPreserving,
	ast.Repeat:          NullPropPreserving,
	ast.Replace:         NullPropPreserving,
	ast.Reverse:         NullPropPreserving,
	ast.Right:           NullPropPreserving,
	ast.RTrim:           NullPropPreserving,
	ast.Rpad:            NullPropPreserving,
	ast.Space:           NullPropPreserving,
	ast.Strcmp:           NullPropPreserving,
	ast.Substring:       NullPropPreserving,
	ast.Substr:          NullPropPreserving,
	ast.SubstringIndex:  NullPropPreserving,
	ast.ToBase64:        NullPropPreserving,
	ast.Trim:            NullPropPreserving,
	ast.Translate:       NullPropPreserving,
	ast.Upper:           NullPropPreserving,
	ast.Ucase:           NullPropPreserving,
	ast.Unhex:           NullPropPreserving,
	ast.WeightString:    NullPropPreserving,
	ast.Soundex:         NullPropPreserving,

	// Regex functions
	ast.RegexpLike:    NullPropPreserving,
	ast.RegexpSubstr:  NullPropPreserving,
	ast.RegexpInStr:   NullPropPreserving,
	ast.RegexpReplace: NullPropPreserving,

	// Date/time functions (NULL in → NULL out)
	ast.AddDate:      NullPropPreserving,
	ast.DateAdd:      NullPropPreserving,
	ast.SubDate:      NullPropPreserving,
	ast.DateSub:      NullPropPreserving,
	ast.AddTime:      NullPropPreserving,
	ast.ConvertTz:    NullPropPreserving,
	ast.Date:         NullPropPreserving,
	ast.DateLiteral:  NullPropPreserving,
	ast.DateFormat:   NullPropPreserving,
	ast.DateDiff:     NullPropPreserving,
	ast.Day:          NullPropPreserving,
	ast.DayName:      NullPropPreserving,
	ast.DayOfMonth:   NullPropPreserving,
	ast.DayOfWeek:    NullPropPreserving,
	ast.DayOfYear:    NullPropPreserving,
	ast.Extract:      NullPropPreserving,
	ast.FromDays:     NullPropPreserving,
	ast.FromUnixTime: NullPropPreserving,
	ast.GetFormat:    NullPropPreserving,
	ast.Hour:         NullPropPreserving,
	ast.MakeDate:     NullPropPreserving,
	ast.MakeTime:     NullPropPreserving,
	ast.MicroSecond:  NullPropPreserving,
	ast.Minute:       NullPropPreserving,
	ast.Month:        NullPropPreserving,
	ast.MonthName:    NullPropPreserving,
	ast.PeriodAdd:    NullPropPreserving,
	ast.PeriodDiff:   NullPropPreserving,
	ast.Quarter:      NullPropPreserving,
	ast.SecToTime:    NullPropPreserving,
	ast.Second:       NullPropPreserving,
	ast.StrToDate:    NullPropPreserving,
	ast.SubTime:      NullPropPreserving,
	ast.Time:         NullPropPreserving,
	ast.TimeLiteral:  NullPropPreserving,
	ast.TimeFormat:   NullPropPreserving,
	ast.TimeToSec:    NullPropPreserving,
	ast.TimeDiff:     NullPropPreserving,
	ast.Timestamp:    NullPropPreserving,
	ast.TimestampLiteral: NullPropPreserving,
	ast.TimestampAdd:     NullPropPreserving,
	ast.TimestampDiff:    NullPropPreserving,
	ast.ToDays:           NullPropPreserving,
	ast.ToSeconds:        NullPropPreserving,
	ast.UnixTimestamp:    NullPropPreserving,
	ast.Week:             NullPropPreserving,
	ast.Weekday:          NullPropPreserving,
	ast.WeekOfYear:       NullPropPreserving,
	ast.Year:             NullPropPreserving,
	ast.YearWeek:         NullPropPreserving,
	ast.LastDay:          NullPropPreserving,

	// Date/time functions that take no arguments (always non-NULL)
	// Classified as Preserving because they never receive NULL input.
	ast.Curdate:          NullPropPreserving,
	ast.CurrentDate:      NullPropPreserving,
	ast.CurrentTime:      NullPropPreserving,
	ast.CurrentTimestamp: NullPropPreserving,
	ast.Curtime:          NullPropPreserving,
	ast.LocalTime:        NullPropPreserving,
	ast.LocalTimestamp:   NullPropPreserving,
	ast.Now:              NullPropPreserving,
	ast.Sysdate:          NullPropPreserving,
	ast.UTCDate:          NullPropPreserving,
	ast.UTCTime:          NullPropPreserving,
	ast.UTCTimestamp:     NullPropPreserving,

	// TSO functions
	ast.TiDBBoundedStaleness: NullPropPreserving,
	ast.TiDBParseTso:         NullPropPreserving,
	ast.TiDBParseTsoLogical:  NullPropPreserving,
	ast.TiDBCurrentTso:       NullPropPreserving,

	// Information functions (mostly 0-arg, always non-NULL)
	ast.ConnectionID:         NullPropPreserving,
	ast.CurrentUser:          NullPropPreserving,
	ast.CurrentRole:          NullPropPreserving,
	ast.Database:             NullPropPreserving,
	ast.CurrentResourceGroup: NullPropPreserving,
	ast.Schema:               NullPropPreserving,
	ast.FoundRows:            NullPropPreserving,
	ast.LastInsertId:         NullPropPreserving,
	ast.User:                 NullPropPreserving,
	ast.Version:              NullPropPreserving,
	ast.Benchmark:            NullPropPreserving,
	ast.Charset:              NullPropPreserving,
	ast.Coercibility:         NullPropPreserving,
	ast.Collation:            NullPropPreserving,
	ast.RowCount:             NullPropPreserving,
	ast.SessionUser:          NullPropPreserving,
	ast.SystemUser:           NullPropPreserving,
	ast.FormatBytes:          NullPropPreserving,
	ast.FormatNanoTime:       NullPropPreserving,

	// Miscellaneous functions
	ast.Sleep:           NullPropPreserving,
	ast.DefaultFunc:     NullPropPreserving,
	ast.InetAton:        NullPropPreserving,
	ast.InetNtoa:        NullPropPreserving,
	ast.Inet6Aton:       NullPropPreserving,
	ast.Inet6Ntoa:       NullPropPreserving,
	ast.IsFreeLock:      NullPropPreserving,
	ast.IsIPv4:          NullPropPreserving,
	ast.IsIPv4Compat:    NullPropPreserving,
	ast.IsIPv4Mapped:    NullPropPreserving,
	ast.IsIPv6:          NullPropPreserving,
	ast.IsUsedLock:      NullPropPreserving,
	ast.IsUUID:          NullPropPreserving,
	ast.NameConst:       NullPropPreserving,
	ast.ReleaseAllLocks: NullPropPreserving,
	ast.UUID:            NullPropPreserving,
	ast.UUIDv4:          NullPropPreserving,
	ast.UUIDv7:          NullPropPreserving,
	ast.UUIDShort:       NullPropPreserving,
	ast.UUIDVersion:     NullPropPreserving,
	ast.UUIDTimestamp:   NullPropPreserving,
	ast.VitessHash:      NullPropPreserving,
	ast.UUIDToBin:       NullPropPreserving,
	ast.BinToUUID:       NullPropPreserving,
	ast.TiDBShard:       NullPropPreserving,
	ast.TiDBRowChecksum: NullPropPreserving,
	ast.Grouping:        NullPropPreserving,
	ast.GetLock:         NullPropPreserving,
	ast.ReleaseLock:     NullPropPreserving,

	// Encryption and compression functions
	ast.AesDecrypt:               NullPropPreserving,
	ast.AesEncrypt:               NullPropPreserving,
	ast.Compress:                 NullPropPreserving,
	ast.Decode:                   NullPropPreserving,
	ast.Encode:                   NullPropPreserving,
	ast.MD5:                      NullPropPreserving,
	ast.PasswordFunc:             NullPropPreserving,
	ast.RandomBytes:              NullPropPreserving,
	ast.SHA1:                     NullPropPreserving,
	ast.SHA:                      NullPropPreserving,
	ast.SHA2:                     NullPropPreserving,
	ast.SM3:                      NullPropPreserving,
	ast.Uncompress:               NullPropPreserving,
	ast.UncompressedLength:       NullPropPreserving,
	ast.ValidatePasswordStrength: NullPropPreserving,

	// JSON functions
	ast.JSONType:          NullPropPreserving,
	ast.JSONExtract:       NullPropPreserving,
	ast.JSONUnquote:       NullPropPreserving,
	ast.JSONSet:           NullPropPreserving,
	ast.JSONInsert:        NullPropPreserving,
	ast.JSONReplace:       NullPropPreserving,
	ast.JSONRemove:        NullPropPreserving,
	ast.JSONMerge:         NullPropPreserving,
	ast.JSONObject:        NullPropHiding, // JSON_OBJECT(k, NULL) → {"k": null}, not NULL
	ast.JSONArray:         NullPropHiding, // JSON_ARRAY(NULL) → [null], not NULL
	ast.JSONMemberOf:      NullPropPreserving,
	ast.JSONContains:      NullPropPreserving,
	ast.JSONOverlaps:      NullPropPreserving,
	ast.JSONContainsPath:  NullPropPreserving,
	ast.JSONValid:         NullPropPreserving,
	ast.JSONArrayAppend:   NullPropPreserving,
	ast.JSONArrayInsert:   NullPropPreserving,
	ast.JSONMergePatch:    NullPropPreserving,
	ast.JSONMergePreserve: NullPropPreserving,
	ast.JSONPretty:        NullPropPreserving,
	ast.JSONQuote:         NullPropPreserving,
	ast.JSONSchemaValid:   NullPropPreserving,
	ast.JSONSearch:        NullPropPreserving,
	ast.JSONStorageFree:   NullPropPreserving,
	ast.JSONStorageSize:   NullPropPreserving,
	ast.JSONDepth:         NullPropPreserving,
	ast.JSONKeys:          NullPropPreserving,
	ast.JSONLength:        NullPropPreserving,

	// Vector functions
	ast.VecDims:                 NullPropPreserving,
	ast.VecL1Distance:           NullPropPreserving,
	ast.VecL2Distance:           NullPropPreserving,
	ast.VecNegativeInnerProduct: NullPropPreserving,
	ast.VecCosineDistance:       NullPropPreserving,
	ast.VecL2Norm:               NullPropPreserving,
	ast.VecFromText:             NullPropPreserving,
	ast.VecAsText:               NullPropPreserving,

	// FTS functions
	ast.FTSMatchWord: NullPropPreserving,

	// TiDB internal functions
	ast.TiDBDecodeKey:       NullPropPreserving,
	ast.TiDBMVCCInfo:        NullPropPreserving,
	ast.TiDBEncodeRecordKey: NullPropPreserving,
	ast.TiDBEncodeIndexKey:  NullPropPreserving,
	ast.TiDBVersion:         NullPropPreserving,
	ast.TiDBIsDDLOwner:      NullPropPreserving,
	ast.TiDBDecodePlan:      NullPropPreserving,
	ast.TiDBDecodeBinaryPlan: NullPropPreserving,
	ast.TiDBDecodeSQLDigests: NullPropPreserving,
	ast.TiDBEncodeSQLDigest:  NullPropPreserving,

	// Sequence functions
	ast.NextVal: NullPropPreserving,
	ast.LastVal: NullPropPreserving,
	ast.SetVal:  NullPropPreserving,

	// ROW constructor
	ast.RowFunc: NullPropHiding, // ROW(NULL, 1) is not NULL

	// Variable functions
	ast.SetVar:   NullPropHiding, // SET @x = NULL returns NULL but is side-effecting
	ast.GetVar:   NullPropHiding, // returns variable value, not input-dependent
	ast.GetParam: NullPropHiding, // prepared statement parameter

	// PI (no args)
	ast.PI: NullPropPreserving,

	// VALUES() function (INSERT ... ON DUPLICATE KEY UPDATE)
	ast.Values: NullPropHiding,

	// Internal charset conversion functions
	InternalFuncFromBinary: NullPropPreserving,
	InternalFuncToBinary:   NullPropPreserving,

	// JSONSumCrc32 is an internal aggregate helper
	ast.JSONSumCrc32: NullPropPreserving,

	// TiDBDecodeBase64Key
	ast.TiDBDecodeBase64Key: NullPropPreserving,
}

// GetNullPropagation returns the null propagation classification for a function.
// Functions not found in the map are conservatively treated as NullPropHiding.
func GetNullPropagation(funcName string) NullPropagation {
	if p, ok := nullPropagationMap[funcName]; ok {
		return p
	}
	return NullPropHiding
}
