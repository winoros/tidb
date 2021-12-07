package function_dependency

import "sort"

// IntSet is used to hold set of vertexes of one side of an edge.
type IntSet map[int]struct{}

// SubsetOf is used to judge whether IntSet itself is a subset of others.
func (is IntSet) SubsetOf (target IntSet) bool {
	for i, _ := range is {
		if _, ok := target[i]; ok {
			continue
		}
		return false
	}
	return true
}

// Intersects is used to judge whether IntSet itself intersects with others.
func (is IntSet) Intersects(target IntSet) bool {
	for i, _ := range is {
		if _, ok := target[i]; ok {
			return true
		}
	}
	return false
}

// Difference is used to exclude the intersection sector away from itself.
func (is IntSet) Difference(target IntSet) {
	for i, _ := range target {
		if _, ok := is[i]; ok {
			delete(is, i)
		}
	}
}

// Union is used to union the IntSet itself with others
func (is IntSet) Union (target IntSet) {
	// deduplicate
	for i, _ := range target {
		if _, ok := is[i]; ok {
			continue
		}
		is[i] = struct{}{}
	}
}

// Equals is used to judge whether two IntSet are semantically equal.
func (is IntSet) Equals (target IntSet) bool {
	if len(is) != len(target) {
		return false
	}
	for i, _ := range target {
		if _, ok := is[i]; !ok {
			return false
		}
	}
	return true
}

func (is IntSet) CopyFrom (target IntSet) {
	for k, _ := range is {
		delete(is, k)
	}
	for k, v := range target {
		is[k] = v
	}
}

func (is IntSet) SortedArray() []int {
	arr := make([]int, 0, len(is))
	for k, _ := range is {
		arr = append(arr, k)
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr
}

type fdEdge struct {
	from IntSet
	to IntSet
	// The value of the strict and eq bool forms the four kind of edges:
	// functional dependency, lax functional dependency, strict equivalence constraint, lax equivalence constraint.
	// And if there's a functional dependency `const` -> `column` exists. We would let the from side be empty.
	strict bool
	equiv bool
}

func NewIntSet() IntSet {
	return make(map[int]struct{})
}

type FDSet struct {
	fdEdges []*fdEdge
}

// closureOf is to find closure of X with respect to F.
// A -> B  =  colSet -> { resultIntSet }
// eg: considering closure F: {A-> CD, B -> E}, and input is {AB}
// res: AB -> {CDE} (AB is included in trivial FD)
func (s *FDSet) closureOf(colSet IntSet) IntSet {
	resultSet := NewIntSet()
	// self included in trivial FD.
	resultSet.Union(colSet)
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict && fd.from.SubsetOf(resultSet) && !fd.to.SubsetOf(resultSet) {
			resultSet.Union(fd.to)
			// If the closure is updated, we redo from the beginning.
			i = -1
		}
	}
	return resultSet
}

func (s *FDSet) inClosure(setA, setB IntSet) bool {
	currentClosure := NewIntSet()
	// self included in trivial FD.
	currentClosure.Union(setA)
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict && fd.from.SubsetOf(currentClosure) && !fd.to.SubsetOf(currentClosure) {
			// once fd.from is subset of setA, which means fd is part of our closure;
			// when fd.to is not subset setA itself, it means inference result is necessary to add;
			currentClosure.Union(fd.to)
			if setB.SubsetOf(currentClosure) {
				return true
			}
			// If the closure is updated, we redo from the beginning.
			i = -1
		}
	}
	return false
}

func (s *FDSet) ReduceCols(colSet IntSet) IntSet {
	// Suppose the colSet is A and B, we have A --> B. Then we only need A since B' value is always determined by A.
	var removed, result IntSet
	result.CopyFrom(colSet)
	for k, v := range colSet {
		removed[k] = v
		delete(result, k)
		// If the removed one is not dependent with the result. We add the bit back.
		if !s.inClosure(result, removed) {
			delete(removed, k)
			result[k] = v
		}
	}
	return result
}

// AddStrictFunctionalDependency is to add functional dependency to the fdGraph, to reduce the edge number,
// we limit the functional dependency when we insert into the set. The key code of insert is like the following codes.
func (s *FDSet) AddStrictFunctionalDependency (from, to IntSet) {
	// trivial FD, refused.
	if to.SubsetOf(from) {
		return
	}

	// exclude the intersection part.
	if to.Intersects(from) {
		to.Difference(from)
	}

	newFD := &fdEdge{
		from: from,
		to: to,
		strict: true,
		equiv: false,
	}

	swapPointer := 0
	added := false
	// the newFD couldn't be superSet of existed one A and be subset of the other existed one B at same time.
	// Because the subset relationship between A and B will be replaced previously.
	for i := range s.fdEdges {
		fd := s.fdEdges[i]
		// If the new one is stronger than the old one. Just replace it.
		if newFD.implies(fd) {
			if added {
				continue
			}
			fd.from = from
			fd.to = to
			fd.strict = true
			fd.equiv = false
		} else if !added {
			// There's a strong one. No need to add.
			if fd.implies(newFD) {
				added = true
			} else if fd.strict == true && fd.equiv == false && fd.from.Equals(from) {
				// We can use the new FD to extend the current one.
				// eg:  A -> BC, A -> CE, they couldn't be the subset of each other, union them.
				// res: A -> BCE
				fd.to.Union(to)
				added = true
			}
		}
		// If the current one is not eliminated, add it to the result.
		s.fdEdges[swapPointer] = s.fdEdges[i]
		swapPointer++
	}
	s.fdEdges = s.fdEdges[:swapPointer]

	// If it's still not added.
	if !added {
		s.fdEdges = append(s.fdEdges, newFD)
	}
}

// implies is used to shrink the edge size, keeping the minimum of the functional dependency set size.
func (e *fdEdge) implies(otherEdge *fdEdge) bool {
	// The given one's from should be larger than the current one and the current one's to should be larger than the given one.
	// A --> C is stronger than AB --> C
	// A --> BC is stronger than A --> C.
	if e.from.SubsetOf(otherEdge.from) && otherEdge.to.SubsetOf(e.to) {
		// The given one should be weaker than the current one.
		// So the given one should not be more strict than the current one.
		// The given one should not be equivalence constraint if the given one is not equivalence constraint.
		if (e.strict || !otherEdge.strict) && (e.equiv || !otherEdge.equiv) {
			return true
		}
	}
	return false
}
