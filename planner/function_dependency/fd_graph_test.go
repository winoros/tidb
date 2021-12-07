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
		from: map[int]struct{}{1: struct {}{}, 2: struct {}{}},  // AB -> CD
		to: map[int]struct{}{3: struct {}{}, 4: struct {}{}},
		strict: true,
		equiv: false,
	}
	fe2 := &fdEdge{
		from: map[int]struct{}{1: struct {}{}, 2: struct {}{}},  // AB -> EF
		to: map[int]struct{}{5: struct {}{}, 6: struct {}{}},
		strict: true,
		equiv: false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2)
	// Add strict edge A -> CDEF.
	fd.AddStrictFunctionalDependency(map[int]struct{}{1: struct {}{}}, map[int]struct{}{3: struct {}{}, 4: struct {}{}, 5: struct {}{}, 6: struct {}{}})
	ass.Equal(len(fd.fdEdges), 1)
}

func TestClosureOf(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from: map[int]struct{}{1: {}, 2: {}},  // AB -> CD
		to: map[int]struct{}{3: {}, 4: {}},
		strict: true,
		equiv: false,
	}
	fe2 := &fdEdge{
		from: map[int]struct{}{1: {}, 2: {}},  // AB -> EF
		to: map[int]struct{}{5: {}, 6: {}},
		strict: true,
		equiv: false,
	}
	fe3 := &fdEdge{
		from: map[int]struct{}{2: {}},         // B -> FG
		to: map[int]struct{}{6: {}, 7: {}},
		strict: true,
		equiv: false,
	}
	fe4 := &fdEdge{
		from: map[int]struct{}{1: {}},         // A -> DEH
		to: map[int]struct{}{4: {}, 5: {}, 8: {}},
		strict: true,
		equiv: false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	// A -> ADEH
	closure := fd.closureOf(map[int]struct{}{1: {}}).SortedArray()
	ass.Equal(len(closure),  4)
	ass.Equal(closure[0], 1)
	ass.Equal(closure[1] ,4)
	ass.Equal(closure[2] ,5)
	ass.Equal(closure[3] ,8)
	// AB -> ABCDEFGH
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3, fe4)
	closure = fd.closureOf(map[int]struct{}{1: {}, 2:{}}).SortedArray()
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


func TestInClosure(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	fd := FDSet{
		fdEdges: []*fdEdge{},
	}
	fe1 := &fdEdge{
		from: map[int]struct{}{1: {}, 2: {}},  // AB -> CD
		to: map[int]struct{}{3: {}, 4: {}},
		strict: true,
		equiv: false,
	}
	fe2 := &fdEdge{
		from: map[int]struct{}{1: {}, 2: {}},  // AB -> EF
		to: map[int]struct{}{5: {}, 6: {}},
		strict: true,
		equiv: false,
	}
	fe3 := &fdEdge{
		from: map[int]struct{}{2: {}},         // B -> FG
		to: map[int]struct{}{6: {}, 7: {}},
		strict: true,
		equiv: false,
	}
	fd.fdEdges = append(fd.fdEdges, fe1, fe2, fe3)
	// A -> F : false (determinants should not be torn apart)
	ass.False(fd.inClosure(map[int]struct{}{1:{}}, map[int]struct{}{6: {}}))
	// B -> G : true (dependency can be torn apart)
	ass.True(fd.inClosure(map[int]struct{}{2:{}}, map[int]struct{}{7: {}}))
	// AB -> E : true (dependency can be torn apart)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2:{}}, map[int]struct{}{5:{}}))
	// AB -> FG: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2:{}}, map[int]struct{}{6:{}, 7:{}}))
	// AB -> DF: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2:{}}, map[int]struct{}{4:{}, 6:{}}))
	// AB -> EG: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2:{}}, map[int]struct{}{5:{}, 7:{}}))
	// AB -> EGH: false (H is not in closure node set)
	ass.False(fd.inClosure(map[int]struct{}{1: {}, 2:{}}, map[int]struct{}{5:{}, 7:{}, 8:{}}))

	fe4 := &fdEdge{
		from: map[int]struct{}{2: {}},         // B -> CH
		to: map[int]struct{}{3: {}, 8: {}},
		strict: true,
		equiv: false,
	}
	fd.fdEdges = append(fd.fdEdges, fe4)
	// AB -> EGH: true (in closure node set)
	ass.True(fd.inClosure(map[int]struct{}{1: {}, 2:{}}, map[int]struct{}{5:{}, 7:{}, 8:{}}))
}
