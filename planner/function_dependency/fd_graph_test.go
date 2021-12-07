package function_dependency

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddStrictFunctionalDependency(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   map[int]struct{}{1: {}}, // AB -> CDEFG
		to:     map[int]struct{}{3: {}, 4: {}, 5: {}, 6: {}, 7: {}},
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   map[int]struct{}{1: {}, 2: {}}, // AB -> CD
		to:     map[int]struct{}{3: {}, 4: {}},
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   map[int]struct{}{1: {}, 2: {}}, // AB -> EF
		to:     map[int]struct{}{5: {}, 6: {}},
		strict: true,
		equiv:  false,
	}
	// fd: AB -> CDEFG implies all of others.
	assertF := func() {
		ass.Equal(len(fd.fdEdges), 1)
		from := fd.fdEdges[0].from.SortedArray()
		ass.Equal(len(from), 1)
		ass.Equal(from[0], 1)
		to := fd.fdEdges[0].to.SortedArray()
		ass.Equal(len(to), 5)
		ass.Equal(to[0], 3)
		ass.Equal(to[1], 4)
		ass.Equal(to[2], 5)
		ass.Equal(to[3], 6)
		ass.Equal(to[4], 7)
	}
	fd.AddStrictFunctionalDependency(fe1.from, fe1.to)
	fd.AddStrictFunctionalDependency(fe2.from, fe2.to)
	fd.AddStrictFunctionalDependency(fe3.from, fe3.to)
	assertF()

	fd.fdEdges = fd.fdEdges[:0]
	fd.AddStrictFunctionalDependency(fe2.from, fe2.to)
	fd.AddStrictFunctionalDependency(fe1.from, fe1.to)
	fd.AddStrictFunctionalDependency(fe3.from, fe3.to)
	assertF()

	// TODO:
	// test reduce col
	// test more edges
}

// Preface Notice:
// For test convenience, we add fdEdge to fdSet directly which is not valid in the procedure.
// Because two difference fdEdge in the fdSet may imply each other which is strictly not permitted in the procedure.
// Use `AddStrictFunctionalDependency` to add the fdEdge to the fdSet in the formal way .
func TestFDSet_ClosureOf(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   map[int]struct{}{1: {}, 2: {}}, // AB -> CD
		to:     map[int]struct{}{3: {}, 4: {}},
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   map[int]struct{}{1: {}, 2: {}}, // AB -> EF
		to:     map[int]struct{}{5: {}, 6: {}},
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   map[int]struct{}{2: {}}, // B -> FG
		to:     map[int]struct{}{6: {}, 7: {}},
		strict: true,
		equiv:  false,
	}
	fe4 := &fdEdge{
		from:   map[int]struct{}{1: {}}, // A -> DEH
		to:     map[int]struct{}{4: {}, 5: {}, 8: {}},
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	// A -> ADEH
	closure := fd.closureOf(map[int]struct{}{1: {}}).SortedArray()
	ass.Equal(len(closure), 4)
	ass.Equal(closure[0], 1)
	ass.Equal(closure[1], 4)
	ass.Equal(closure[2], 5)
	ass.Equal(closure[3], 8)
	// AB -> ABCDEFGH
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	closure = fd.closureOf(map[int]struct{}{1: {}, 2: {}}).SortedArray()
	ass.Equal(len(closure), 8)
	ass.Equal(closure[0], 1)
	ass.Equal(closure[1], 2)
	ass.Equal(closure[2], 3)
	ass.Equal(closure[3], 4)
	ass.Equal(closure[4], 5)
	ass.Equal(closure[5], 6)
	ass.Equal(closure[6], 7)
	ass.Equal(closure[7], 8)
}

func TestFDSet_ReduceCols(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   map[int]struct{}{1: {}}, // A -> CD
		to:     map[int]struct{}{3: {}, 4: {}},
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   map[int]struct{}{3: {}}, // C -> DE
		to:     map[int]struct{}{4: {}, 5: {}},
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   map[int]struct{}{3: {}, 5: {}}, // CE -> B
		to:     map[int]struct{}{2: {}},
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3)
	res := fd.ReduceCols(map[int]struct{}{1: {}, 2: {}}).SortedArray()
	ass.Equal(len(res), 1)
	ass.Equal(res[0], 1)
}

func TestFDSet_InClosure(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from:   map[int]struct{}{1: {}, 2: {}}, // AB -> CD
		to:     map[int]struct{}{3: {}, 4: {}},
		strict: true,
		equiv:  false,
	}
	fe2 := &fdEdge{
		from:   map[int]struct{}{1: {}, 2: {}}, // AB -> EF
		to:     map[int]struct{}{5: {}, 6: {}},
		strict: true,
		equiv:  false,
	}
	fe3 := &fdEdge{
		from:   map[int]struct{}{2: {}}, // B -> FG
		to:     map[int]struct{}{6: {}, 7: {}},
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3)
	// A -> F : false (determinants should not be torn apart)
	ass.False(fd.inClosure(map[int]struct{}{1: {}}, map[int]struct{}{6: {}}))
	// B -> G : true (dependency can be torn apart)
	ass.True(fd.inClosure(map[int]struct{}{2: {}}, map[int]struct{}{7: {}}))
	// AB -> E : true (dependency can be torn apart)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2: {}}, map[int]struct{}{5: {}}))
	// AB -> FG: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2: {}}, map[int]struct{}{6: {}, 7: {}}))
	// AB -> DF: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2: {}}, map[int]struct{}{4: {}, 6: {}}))
	// AB -> EG: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2: {}}, map[int]struct{}{5: {}, 7: {}}))
	// AB -> EGH: false (H is not in closure node set)
	ass.False(fd.inClosure(map[int]struct{}{1: {}, 2: {}}, map[int]struct{}{5: {}, 7: {}, 8: {}}))

	fe4 := &fdEdge{
		from:   map[int]struct{}{2: {}}, // B -> CH
		to:     map[int]struct{}{3: {}, 8: {}},
		strict: true,
		equiv:  false,
	}
	fd.fdEdges = append(fd.fdEdges, fe4)
	// AB -> EGH: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2: {}}, map[int]struct{}{5: {}, 7: {}, 8: {}}))
}
