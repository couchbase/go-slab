//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// Package slab provides a 100% golang slab allocator for byte slices.
package slab

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

// An opaque reference to bytes managed by an Arena.  See
// Arena.BufToLoc/LocToBuf().  A Loc struct is GC friendly in that a
// Loc does not have direct pointer fields into the Arena's memory
// that the GC's scanner must traverse.
type Loc struct {
	slabClassIndex int
	slabIndex      int
	chunkIndex     int
	bufStart       int
	bufLen         int
}

// NilLoc returns a Loc where Loc.IsNil() is true.
func NilLoc() Loc {
	return nilLoc
}

var nilLoc = Loc{-1, -1, -1, -1, -1} // A sentinel.

// IsNil returns true if the Loc came from NilLoc().
func (cl Loc) IsNil() bool {
	return cl.slabClassIndex < 0 && cl.slabIndex < 0 &&
		cl.chunkIndex < 0 && cl.bufStart < 0 && cl.bufLen < 0
}

// Slice returns a Loc that a represents a different slice of the
// backing buffer, where the bufStart and bufLen are relative to the
// backing buffer.  Does not change the ref-count of the underlying
// buffer.
//
// NOTE: Many API's (such as BufToLoc) do not correctly handle Loc's
// with non-zero bufStart, so use sliced Loc's with caution.
func (cl Loc) Slice(bufStart, bufLen int) Loc {
	rv := cl // Makes a copy.
	rv.bufStart = bufStart
	rv.bufLen = bufLen
	return rv
}

// An Arena manages a set of slab classes and memory.
type Arena struct {
	growthFactor float64
	slabClasses  []slabClass // slabClasses's chunkSizes grow by growthFactor.
	slabMagic    int32       // Magic # suffix on each slab memory []byte.
	slabSize     int
	malloc       func(size int) []byte // App-specific allocator.

	totAllocs           int64
	totAddRefs          int64
	totDecRefs          int64
	totDecRefZeroes     int64 // Inc'ed when a ref-count reaches zero.
	totGetNexts         int64
	totSetNexts         int64
	totMallocs          int64
	totMallocErrs       int64
	totTooBigErrs       int64
	totAddSlabErrs      int64
	totPushFreeChunks   int64 // Inc'ed when chunk added to free list.
	totPopFreeChunks    int64 // Inc'ed when chunk removed from free list.
	totPopFreeChunkErrs int64
}

type slabClass struct {
	slabs     []*slab // A growing array of slabs.
	chunkSize int     // Each slab is sliced into fixed-sized chunks.
	chunkFree Loc     // Chunks are tracked in a free-list per slabClass.

	numChunks     int64
	numChunksFree int64
}

type slab struct {
	// len(memory) == slabSize + slabMemoryFooterLen.
	memory []byte

	// Matching array of chunk metadata, and len(memory) == len(chunks).
	chunks []chunk
}

// Based on slabClassIndex + slabIndex + slabMagic.
const slabMemoryFooterLen int = 4 + 4 + 4

type chunk struct {
	refs int32 // Ref-count.
	self Loc   // The self is the Loc for this chunk.
	next Loc   // Used when chunk is in the free-list or when chained.
}

// NewArena returns an Arena to manage byte slice memory based on a
// slab allocator approach.
//
// The startChunkSize and slabSize should be > 0.
// The growthFactor should be > 1.0.
// The malloc() func is invoked when Arena needs memory for a new slab.
// When malloc() is nil, then Arena defaults to make([]byte, size).
func NewArena(startChunkSize int, slabSize int, growthFactor float64,
	malloc func(size int) []byte) *Arena {
	if malloc == nil {
		malloc = defaultMalloc
	}
	s := &Arena{
		growthFactor: growthFactor,
		slabMagic:    rand.Int31(),
		slabSize:     slabSize,
		malloc:       malloc,
	}
	s.addSlabClass(startChunkSize)
	return s
}

func defaultMalloc(size int) []byte {
	return make([]byte, size)
}

// Alloc may return nil on errors, such as if no more free chunks are
// available and new slab memory was not allocatable (such as if
// malloc() returns nil).  The returned buf may not be append()'ed to
// for growth.  The returned buf must be DecRef()'ed for memory reuse.
func (s *Arena) Alloc(bufLen int) (buf []byte) {
	sc, chunk := s.allocChunk(bufLen)
	if sc == nil || chunk == nil {
		return nil
	}
	return sc.chunkMem(chunk)[0:bufLen]
}

// Owns returns true if this Arena owns the buf.
func (s *Arena) Owns(buf []byte) bool {
	sc, c := s.bufChunk(buf)
	return sc != nil && c != nil
}

// AddRef increase the ref count on a buf.  The input buf must be from
// an Alloc() from the same Arena.
func (s *Arena) AddRef(buf []byte) {
	s.totAddRefs++
	sc, c := s.bufChunk(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	c.addRef()
}

// DecRef decreases the ref count on a buf.  The input buf must be
// from an Alloc() from the same Arena.  Once the buf's ref-count
// drops to 0, the Arena may reuse the buf.  Returns true if this was
// the last DecRef() invocation (ref count reached 0).
func (s *Arena) DecRef(buf []byte) bool {
	s.totDecRefs++
	sc, c := s.bufChunk(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	return s.decRef(sc, c)
}

// GetNext returns the next chained buf for the given input buf.  The
// buf's managed by an Arena can be chained.  The returned bufNext may
// be nil.  When the returned bufNext is non-nil, the caller owns a
// ref-count on bufNext and must invoke DecRef(bufNext) when the
// caller is finished using bufNext.
func (s *Arena) GetNext(buf []byte) (bufNext []byte) {
	s.totGetNexts++
	sc, c := s.bufChunk(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	if c.refs <= 0 {
		panic(fmt.Sprintf("unexpected ref-count during GetNext: %#v", c))
	}

	scNext, cNext := s.chunk(c.next)
	if scNext == nil || cNext == nil {
		return nil
	}

	cNext.addRef()

	return scNext.chunkMem(cNext)[c.next.bufStart : c.next.bufStart+c.next.bufLen]
}

// SetNext associates the next chain buf following the input buf to be
// bufNext.  The buf's from an Arena can be chained, where buf will
// own an AddRef() on bufNext.  When buf's ref-count goes to zero, it
// will call DecRef() on bufNext.  The bufNext may be nil.  The
// bufNext must have start position 0 (or bufStart of 0) with respect
// to its backing buffer.
func (s *Arena) SetNext(buf, bufNext []byte) {
	s.totSetNexts++
	sc, c := s.bufChunk(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	if c.refs <= 0 {
		panic(fmt.Sprintf("refs <= 0 during SetNext: %#v", c))
	}

	scOldNext, cOldNext := s.chunk(c.next)
	if scOldNext != nil && cOldNext != nil {
		s.decRef(scOldNext, cOldNext)
	}

	c.next = nilLoc
	if bufNext != nil {
		scNewNext, cNewNext := s.bufChunk(bufNext)
		if scNewNext == nil || cNewNext == nil {
			panic("bufNext not from this arena")
		}
		cNewNext.addRef()

		c.next = cNewNext.self
		c.next.bufStart = 0
		c.next.bufLen = len(bufNext)
	}
}

// BufToLoc returns a Loc that represents an Arena-managed buf.  Does
// not affect the reference count of the buf.  The buf slice must have
// start position 0 (or bufStart of 0).
func (s *Arena) BufToLoc(buf []byte) Loc {
	sc, c := s.bufChunk(buf)
	if sc == nil || c == nil {
		return NilLoc()
	}

	var loc = c.self // Makes a copy.
	loc.bufStart = 0
	loc.bufLen = len(buf)
	return loc
}

// LocToBuf returns a buf for an Arena-managed Loc.  Does not affect
// the reference count of the buf.  The Loc may have come from
// Loc.Slice().
func (s *Arena) LocToBuf(loc Loc) []byte {
	sc, chunk := s.chunk(loc)
	if sc == nil || chunk == nil {
		return nil
	}
	return sc.chunkMem(chunk)[loc.bufStart : loc.bufStart+loc.bufLen]
}

func (s *Arena) LocAddRef(loc Loc) {
	s.totAddRefs++
	sc, chunk := s.chunk(loc)
	if sc == nil || chunk == nil {
		return
	}
	chunk.addRef()
}

func (s *Arena) LocDecRef(loc Loc) {
	s.totDecRefs++
	sc, chunk := s.chunk(loc)
	if sc == nil || chunk == nil {
		return
	}
	s.decRef(sc, chunk)
}

// ---------------------------------------------------------------

func (s *Arena) allocChunk(bufLen int) (*slabClass, *chunk) {
	s.totAllocs++

	if bufLen > s.slabSize {
		s.totTooBigErrs++
		return nil, nil
	}

	slabClassIndex := s.findSlabClassIndex(bufLen)
	sc := &(s.slabClasses[slabClassIndex])
	if sc.chunkFree.IsNil() {
		if !s.addSlab(slabClassIndex, s.slabSize, s.slabMagic) {
			s.totAddSlabErrs++
			return nil, nil
		}
	}

	s.totPopFreeChunks++
	chunk := sc.popFreeChunk()
	if chunk == nil {
		s.totPopFreeChunkErrs++
		return nil, nil
	}

	return sc, chunk
}

func (s *Arena) findSlabClassIndex(bufLen int) int {
	i := sort.Search(len(s.slabClasses),
		func(i int) bool { return bufLen <= s.slabClasses[i].chunkSize })
	if i >= len(s.slabClasses) {
		slabClass := &(s.slabClasses[len(s.slabClasses)-1])
		nextChunkSize := float64(slabClass.chunkSize) * s.growthFactor
		s.addSlabClass(int(math.Ceil(nextChunkSize)))
		return s.findSlabClassIndex(bufLen)
	}
	return i
}

func (s *Arena) addSlabClass(chunkSize int) {
	s.slabClasses = append(s.slabClasses, slabClass{
		chunkSize: chunkSize,
		chunkFree: nilLoc,
	})
}

func (s *Arena) addSlab(
	slabClassIndex, slabSize int, slabMagic int32) bool {
	sc := &(s.slabClasses[slabClassIndex])

	chunksPerSlab := slabSize / sc.chunkSize
	if chunksPerSlab <= 0 {
		chunksPerSlab = 1
	}

	slabIndex := len(sc.slabs)

	s.totMallocs++
	// Re-multiplying to avoid any extra fractional chunk memory.
	memorySize := (sc.chunkSize * chunksPerSlab) + slabMemoryFooterLen
	memory := s.malloc(memorySize)
	if memory == nil {
		s.totMallocErrs++
		return false
	}

	slab := &slab{
		memory: memory,
		chunks: make([]chunk, chunksPerSlab),
	}

	footer := slab.memory[len(slab.memory)-slabMemoryFooterLen:]
	binary.BigEndian.PutUint32(footer[0:4], uint32(slabClassIndex))
	binary.BigEndian.PutUint32(footer[4:8], uint32(slabIndex))
	binary.BigEndian.PutUint32(footer[8:12], uint32(slabMagic))

	sc.slabs = append(sc.slabs, slab)

	for i := 0; i < len(slab.chunks); i++ {
		c := &(slab.chunks[i])
		c.self.slabClassIndex = slabClassIndex
		c.self.slabIndex = slabIndex
		c.self.chunkIndex = i
		c.self.bufStart = 0
		c.self.bufLen = sc.chunkSize
		sc.pushFreeChunk(c)
	}
	sc.numChunks += int64(len(slab.chunks))

	return true
}

func (sc *slabClass) pushFreeChunk(c *chunk) {
	if c.refs != 0 {
		panic(fmt.Sprintf("pushFreeChunk() non-zero refs: %v", c.refs))
	}
	c.next = sc.chunkFree
	sc.chunkFree = c.self
	sc.numChunksFree++
}

func (sc *slabClass) popFreeChunk() *chunk {
	if sc.chunkFree.IsNil() {
		panic("popFreeChunk() when chunkFree is nil")
	}
	c := sc.chunk(sc.chunkFree)
	if c.refs != 0 {
		panic(fmt.Sprintf("popFreeChunk() non-zero refs: %v", c.refs))
	}
	c.refs = 1
	sc.chunkFree = c.next
	c.next = nilLoc
	sc.numChunksFree--
	if sc.numChunksFree < 0 {
		panic("popFreeChunk() got < 0 numChunksFree")
	}
	return c
}

func (sc *slabClass) chunkMem(c *chunk) []byte {
	if c == nil || c.self.IsNil() {
		return nil
	}
	beg := sc.chunkSize * c.self.chunkIndex
	return sc.slabs[c.self.slabIndex].memory[beg : beg+sc.chunkSize]
}

func (sc *slabClass) chunk(cl Loc) *chunk {
	if cl.IsNil() {
		return nil
	}
	return &(sc.slabs[cl.slabIndex].chunks[cl.chunkIndex])
}

func (s *Arena) chunk(cl Loc) (*slabClass, *chunk) {
	if cl.IsNil() {
		return nil, nil
	}
	sc := &(s.slabClasses[cl.slabClassIndex])
	return sc, sc.chunk(cl)
}

// Determine the slabClass & chunk for an Arena managed buf []byte.
func (s *Arena) bufChunk(buf []byte) (*slabClass, *chunk) {
	if buf == nil || cap(buf) <= slabMemoryFooterLen {
		return nil, nil
	}

	rest := buf[:cap(buf)]
	footerDistance := len(rest) - slabMemoryFooterLen
	footer := rest[footerDistance:]

	slabClassIndex := binary.BigEndian.Uint32(footer[0:4])
	slabIndex := binary.BigEndian.Uint32(footer[4:8])
	slabMagic := binary.BigEndian.Uint32(footer[8:12])
	if slabMagic != uint32(s.slabMagic) {
		return nil, nil
	}

	sc := &(s.slabClasses[slabClassIndex])
	slab := sc.slabs[slabIndex]
	chunkIndex := len(slab.chunks) -
		int(math.Ceil(float64(footerDistance)/float64(sc.chunkSize)))

	return sc, &(slab.chunks[chunkIndex])
}

func (c *chunk) addRef() *chunk {
	c.refs++
	if c.refs <= 1 {
		panic(fmt.Sprintf("refs <= 1 during addRef: %#v", c))
	}
	return c
}

func (s *Arena) decRef(sc *slabClass, c *chunk) bool {
	c.refs--
	if c.refs < 0 {
		panic(fmt.Sprintf("refs < 0 during decRef: %#v", c))
	}
	if c.refs == 0 {
		s.totDecRefZeroes++
		scNext, cNext := s.chunk(c.next)
		if scNext != nil && cNext != nil {
			s.decRef(scNext, cNext)
		}
		c.next = nilLoc
		s.totPushFreeChunks++
		sc.pushFreeChunk(c)
		return true
	}
	return false
}

// Stats fills an input map with runtime metrics about the Arena.
func (s *Arena) Stats(m map[string]int64) map[string]int64 {
	m["totSlabClasses"] = int64(len(s.slabClasses))
	m["totAllocs"] = s.totAllocs
	m["totAddRefs"] = s.totAddRefs
	m["totDecRefs"] = s.totDecRefs
	m["totDecRefZeroes"] = s.totDecRefZeroes
	m["totGetNexts"] = s.totGetNexts
	m["totSetNexts"] = s.totSetNexts
	m["totMallocs"] = s.totMallocs
	m["totMallocErrs"] = s.totMallocErrs
	m["totTooBigErrs"] = s.totTooBigErrs
	m["totAddSlabErrs"] = s.totAddSlabErrs
	m["totPushFreeChunks"] = s.totPushFreeChunks
	m["totPopFreeChunks"] = s.totPopFreeChunks
	m["totPopFreeChunkErrs"] = s.totPopFreeChunkErrs
	for i, sc := range s.slabClasses {
		prefix := fmt.Sprintf("slabClass-%06d-", i)
		m[prefix+"numSlabs"] = int64(len(sc.slabs))
		m[prefix+"chunkSize"] = int64(sc.chunkSize)
		m[prefix+"numChunks"] = int64(sc.numChunks)
		m[prefix+"numChunksFree"] = int64(sc.numChunksFree)
		m[prefix+"numChunksInUse"] = int64(sc.numChunks - sc.numChunksFree)
	}
	return m
}
