package functional_dependency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuncDeps_ConstCols(t *testing.T) {
	fd := &FDSet{}
	ass := assert.New(t)
	require.Equal(t, "()", fd.ConstantCols().String())
	fd.AddConstants(NewFastIntSet(1, 2))
	require.Equal(t, "(1,2)", fd.ConstantCols().String())

	fd2 := makeAbcdeFD(ass)
	require.Equal(t, "()", fd2.ConstantCols().String())
	fd2.AddConstants(NewFastIntSet(1, 2))
	require.Equal(t, "(1,2)", fd.ConstantCols().String())
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
//   CREATE UNIQUE INDEX ON abcde (b, c)
func makeAbcdeFD(ass *assert.Assertions) *FDSet {
	// Set Key to all cols to start, and ensure it's overridden in AddStrictKey.
	allCols := NewFastIntSet(1, 2, 3, 4, 5)
	abcde := &FDSet{}
	abcde.AddStrictFunctionalDependency(NewFastIntSet(1), allCols)
	abcde.AddLaxFunctionalDependency(NewFastIntSet(2, 3), allCols)

	testColsAreStrictKey(ass, abcde, NewFastIntSet(1), allCols, true)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(2, 3), allCols, false)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(1, 2), allCols, true)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(1, 2, 3, 4, 5), allCols, true)
	testColsAreStrictKey(ass, abcde, NewFastIntSet(4, 5), allCols, false)
	testColsAreLaxKey(ass, abcde, NewFastIntSet(2, 3), allCols, true)
	return abcde
}

func testColsAreStrictKey(ass *assert.Assertions, fd *FDSet, cols, allCols FastIntSet, is bool) {
	closure := fd.closureOfStrict(cols)
	ass.Equal(allCols.SubsetOf(closure), is)
}

func testColsAreLaxKey(ass *assert.Assertions, fd *FDSet, cols, allCols FastIntSet, is bool) {
	closure := fd.closureOfLax(cols)
	ass.Equal(allCols.SubsetOf(closure), is)
}

// Other tests also exercise the ColsAreKey methods.
func TestFuncDeps_ColsAreKey(t *testing.T) {
	// CREATE TABLE abcde (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)
	// CREATE UNIQUE INDEX ON abcde (b, c)
	// CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
	// This case wouldn't actually happen with a real world query.
	ass := assert.New(t)
	var loj FDSet
	preservedCols := NewFastIntSet(1, 2, 3, 4, 5)
	nullExtendedCols := NewFastIntSet(10, 11, 12, 13, 14)
	abcde := makeAbcdeFD(ass)
	mnpq := makeMnpqFD(ass)
	mnpq.AddStrictFunctionalDependency(NewFastIntSet(12, 13), NewFastIntSet(14))
	loj = *abcde
	loj.MakeCartesianProduct(mnpq)
	loj.AddConstants(NewFastIntSet(3))
	loj.MakeLeftOuter(abcde, &FDSet{}, preservedCols, nullExtendedCols, NewFastIntSet(1, 10, 11))
	loj.AddEquivalence(NewFastIntSet(1), NewFastIntSet(10))

	testcases := []struct {
		cols   FastIntSet
		strict bool
		lax    bool
	}{
		{cols: NewFastIntSet(1, 2, 3, 4, 5, 10, 11, 12, 13, 14), strict: true, lax: true},
		{cols: NewFastIntSet(1, 2, 3, 4, 5, 10, 12, 13, 14), strict: false, lax: false},
		{cols: NewFastIntSet(1, 11), strict: true, lax: true},
		{cols: NewFastIntSet(10, 11), strict: true, lax: true},
		{cols: NewFastIntSet(1), strict: false, lax: false},
		{cols: NewFastIntSet(10), strict: false, lax: false},
		{cols: NewFastIntSet(11), strict: false, lax: false},
		{cols: NewFastIntSet(), strict: false, lax: false},

		// This case is interesting: if we take into account that 3 is a constant,
		// we could put 2 and 3 together and use (2,3)~~>(1,4,5) and (1)==(10) to
		// prove that (2,3) is a lax key. But this is only true when that constant
		// value for 3 is not NULL. We would have to pass non-null information to
		// the check. See #42731.
		{cols: NewFastIntSet(2, 11), strict: false, lax: false},
	}

	for _, tc := range testcases {
		testColsAreStrictKey(ass, &loj, tc.cols, loj.AllCols(), tc.strict)
		testColsAreLaxKey(ass, &loj, tc.cols, loj.AllCols(), tc.lax)
	}
}

// Construct base table FD from figure 3.3, page 114:
//   CREATE TABLE mnpq (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))
func makeMnpqFD(ass *assert.Assertions) *FDSet {
	allCols := NewFastIntSet(10, 11, 12, 13)
	mnpq := &FDSet{}
	mnpq.AddStrictFunctionalDependency(NewFastIntSet(10, 11), allCols)
	mnpq.MakeNotNull(NewFastIntSet(10, 11))
	testColsAreStrictKey(ass, mnpq, NewFastIntSet(10), allCols, false)
	testColsAreStrictKey(ass, mnpq, NewFastIntSet(10, 11), allCols, true)
	testColsAreStrictKey(ass, mnpq, NewFastIntSet(10, 11, 12), allCols, true)
	return mnpq
}
