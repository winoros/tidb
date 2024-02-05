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

package hasher

import (
	"encoding/binary"
	"hash"
	"hash/fnv"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
)

type Result = uint64

var ZeroHash = uint64(0)

type Hasher struct {
	innerHasher hash.Hash64
	tmpBytes    []byte
}

func (h *Hasher) HashBytes(b []byte) error {
	// nolint: errcheck
	h.innerHasher.Write(b)
	return nil
}

func (h *Hasher) HashByte(b byte) error {
	h.tmpBytes = h.tmpBytes[:0]
	h.tmpBytes = append(h.tmpBytes, b)
	// nolint: errcheck
	h.innerHasher.Write(h.tmpBytes)
	return nil
}

func (h *Hasher) HashInt64(x int64) error {
	h.tmpBytes = h.tmpBytes[:0]
	// nolint: errcheck
	binary.LittleEndian.AppendUint64(h.tmpBytes, uint64(x))
	h.innerHasher.Write(h.tmpBytes)
	return nil
}

func (h *Hasher) HashUint64(x uint64) error {
	h.tmpBytes = h.tmpBytes[:0]
	binary.LittleEndian.AppendUint64(h.tmpBytes, x)
	// nolint: errcheck
	h.innerHasher.Write(h.tmpBytes)
	return nil
}

func (h *Hasher) HashDatum(x *types.Datum) error {
	h.HashByte(x.Kind())
	switch x.Kind() {
	case types.KindNull:
		return nil
	case types.KindInt64:
		h.HashInt64(x.GetInt64())
	case types.KindUint64, types.KindFloat32, types.KindFloat64:
		h.HashUint64(x.GetUint64())
	case types.KindString:
		h.HashBytes(x.GetBytes())
		h.HashBytes(hack.Slice(x.Collation()))
	case types.KindBytes:
		h.HashBytes(x.GetBytes())
	}
	return nil
}

func (h *Hasher) HashHashResult(x Result) error {
	return h.HashUint64(x)
}

func (h *Hasher) Reset() {
	h.innerHasher.Reset()
}

func (h *Hasher) Result() Result {
	return h.innerHasher.Sum64()
}

func NewHasher() *Hasher {
	return &Hasher{
		innerHasher: fnv.New64a(),
		tmpBytes:    make([]byte, 4),
	}
}
