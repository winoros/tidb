package function_dependency

// IntSet is used to hold set of vertexes of one side of an edge.
type IntSet []int

func (is IntSet) SubsetOf (target IntSet) bool {
	for i := range is {
		found := false
		for j := range target {
			if i == j {
				found = true
				break
			}
		}
		if found {
			continue
		}
		return false
	}
	return true
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
	return []int{}
}

type FDSet struct {
	fdEdges []*fdEdge
}

func (s *FDSet) closureOf(colSet IntSet) IntSet {
	resultSet := NewIntSet()
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict && fd.from.SubsetOf(colSet) && !fd.to.SubsetOf(colSet) {
			resultSet.Union(fd.to)
			// If the closure is updated, we redo from the beginning.
			i = -1
		}
	}
	return resultSet
}

func (s *FDSet) inClosure(setA, setB IntSet) bool {
	var currentClosure IntSet
	for i := 0; i < len(s.fdEdges); i++ {
		fd := s.fdEdges[i]
		if fd.strict && fd.from.SubsetOf(setA) && !fd.to.SubsetOf(setA) {
			resultSet.Union(fd.to)
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
	for i := colSet.FirstBit(); i != colSet.End(); i = colSet.NextBit(i) {
		removed.Insert(i)
		result.Remove(i)
		// If the removed one is not dependent with the result. We add the bit back.
		if !s.inClosure(result, removed) {
			removed.Remove(i)
			result.Insert(i)
		}
	}
	return result
}

// AddStrictFunctionalDependency is to add functional dependency to the fdGraph, to reduce the edge number,
// we limit the functional dependency when we insert into the set. The key code of insert is like the following codes.
func (s *FDSet) AddStrictFunctionalDependency (from, to IntSet) {
	if to.SubsetOf(from) {
		return
	}

	if to.Intersects(from) {
		to.Difference(from)
	}

	newFD := fdEdge{
		from: from,
		to: to,
		strict: true,
		equiv: false,
	}
	swapPointer := 0
	added := false
	for i := range s.fdEdges {
		fd := &s.fdEdges[i]
		// If the new one is strong than the old one. Just replace it.
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
			} else if fd.strict = true && fd.equiv == false && fd.from.Equals(from) {
				// We can use the new FD to extend the current one.
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
