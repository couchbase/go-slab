package slab

import (
	"testing"
)

func TestBasics(t *testing.T) {
	s := SynchronizedArena(NewSlabArena(1, 1024, 2))
	if s == nil {
		t.Errorf("expected new slab arena to work")
	}
	a := s.Alloc(1)
	if a == nil {
		t.Errorf("expected alloc to work")
	}
	if len(a) != 1 {
		t.Errorf("expected alloc to give right size buf")
	}
	if cap(a) != 1 + SLAB_MEMORY_FOOTER_LEN {
		t.Errorf("expected alloc cap to match algorithm, got: %v vs %v",
			cap(a), 1 + SLAB_MEMORY_FOOTER_LEN)
	}
	a[0] = 66
	s.DecRef(a)
	b := s.Alloc(1)
	if b == nil {
		t.Errorf("expected alloc to work")
	}
	if len(b) != 1 {
		t.Errorf("expected alloc to give right size buf")
	}
	if cap(b) != 1 + SLAB_MEMORY_FOOTER_LEN {
		t.Errorf("expected alloc cap to match algorithm, got: %v vs %v",
			cap(b), 1 + SLAB_MEMORY_FOOTER_LEN)
	}
	if b[0] != 66 {
		t.Errorf("expected alloc to return last freed buf")
	}
}

func TestSlabClassGrowth(t *testing.T) {
	s := NewSlabArena(1, 8, 2).(*slabArena)
	expectSlabClasses := func(numSlabClasses int) {
		if len(s.slabClasses) != numSlabClasses {
			t.Errorf("expected %v slab classses, got: %v",
				numSlabClasses, len(s.slabClasses))
		}
	}
	expectSlabClasses(1)
	s.Alloc(1)
	expectSlabClasses(1)
	s.Alloc(1)
	expectSlabClasses(1)
	s.Alloc(2)
	expectSlabClasses(2)
	s.Alloc(1)
	s.Alloc(2)
	expectSlabClasses(2)
	s.Alloc(3)
	s.Alloc(4)
	expectSlabClasses(3)
	s.Alloc(5)
	s.Alloc(8)
	expectSlabClasses(4)
}

func TestDecRef(t *testing.T) {
	s := NewSlabArena(1, 8, 2).(*slabArena)
	expectSlabClasses := func(numSlabClasses int) {
		if len(s.slabClasses) != numSlabClasses {
			t.Errorf("expected %v slab classses, got: %v",
				numSlabClasses, len(s.slabClasses))
		}
	}
	a := make([][]byte, 128)
	for j := 0; j < 100; j++ {
		for i := 0; i < len(a); i++ {
			a[i] = s.Alloc(i % 8)
		}
		for i := 0; i < len(a); i++ {
			s.DecRef(a[i])
		}
	}
	expectSlabClasses(4)
}

func TestAddRef(t *testing.T) {
	s := NewSlabArena(1, 1, 2).(*slabArena)
	if !s.slabClasses[0].chunkFree.isEmpty() {
		t.Errorf("expected no free chunks")
	}
	a := s.Alloc(1)
	if !s.slabClasses[0].chunkFree.isEmpty() {
		t.Errorf("expected no free chunks")
	}
	s.AddRef(a)
	if !s.slabClasses[0].chunkFree.isEmpty() {
		t.Errorf("expected no free chunks")
	}
	s.DecRef(a)
	if !s.slabClasses[0].chunkFree.isEmpty() {
		t.Errorf("expected no free chunks")
	}
	s.DecRef(a)
	if s.slabClasses[0].chunkFree.isEmpty() {
		t.Errorf("expected 1 free chunk")
	}
}
