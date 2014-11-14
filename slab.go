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

// Package slab provides a slab allocator for golang.
package slab

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

// An Arena manages a set of slab classes and memory.
type Arena struct {
	growthFactor float64
	slabClasses  []slabClass // The chunkSizes of slabClasses grows by growthFactor.
	slabMagic    int32       // Magic number at the end of each slab memory []byte.
	slabSize     int
	malloc       func(size int) []byte // App-specific allocator; may be nil.

	numAllocs         int64
	numAddRefs        int64
	numDecRefs        int64
	numGetNexts       int64
	numSetNexts       int64
	numMallocs        int64
	numMallocErrs     int64
	numTooBigErrs     int64
	numNoChunkMemErrs int64
}

type slabClass struct {
	slabs     []*slab  // A growing array of slabs.
	chunkSize int      // Each slab is sliced into fixed-sized chunks.
	chunkFree chunkLoc // Chunks are tracked in a free-list per slabClass.

	numChunks     int64
	numChunksFree int64
}

type slab struct {
	memory []byte  // len(memory) == slabSize + slab_memory_footer_len.
	chunks []chunk // Parallel array of chunk metadata.
}

const slab_memory_footer_len int = 4 + 4 + 4 // slabClassIndex + slabIndex + slabMagic.

type chunkLoc struct {
	slabClassIndex int
	slabIndex      int
	chunkIndex     int
	chunkSize      int
}

var empty_chunkLoc = chunkLoc{-1, -1, -1, -1} // A sentinel.

func (cl *chunkLoc) isEmpty() bool {
	return cl.slabClassIndex == -1 && cl.slabIndex == -1 &&
		cl.chunkIndex == -1 && cl.chunkSize == -1
}

type chunk struct {
	refs int32    // Ref-count.
	self chunkLoc // The self is the chunkLoc for this chunk.
	next chunkLoc // Used when the chunk is in the free-list or when chained.
}

// NewArena returns an Arena to manage memory based on a slab allocator approach.
// The startChunkSize and slabSize should be > 0.
// The growthFactor should be > 1.0.
// The malloc() func will be invoked when the Arena needs more memory for a new slab.
// The malloc() may be nil, in which case the Arena defaults to make([]byte, size).
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

// Alloc may return nil on errors, such as if no more free chunks
// are available and new slab memory was not allocatable (such as if
// malloc() returns nil).
func (s *Arena) Alloc(bufSize int) (buf []byte) {
	s.numAllocs++
	if bufSize > s.slabSize {
		s.numTooBigErrs++
		return nil
	}
	chunkMem := s.assignChunkMem(s.findSlabClassIndex(bufSize))
	if chunkMem == nil {
		s.numNoChunkMemErrs++
		return nil
	}
	return chunkMem[0:bufSize]
}

// AddRef increase the ref count on an input buf.  The input buf must
// be from an Alloc() from the same Arena.
func (s *Arena) AddRef(buf []byte) {
	s.numAddRefs++
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	c.addRef()
}

// DecRef decreases the ref count on an input buf.  The input buf must
// be from an Alloc() from the same Arena.  Once the buf's ref-count
// drops to 0, the Arena may reuse the buf.  Returns true if this was
// the last DecRef() invocation (ref count reached 0).
func (s *Arena) DecRef(buf []byte) bool {
	s.numDecRefs++
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	return s.decRef(sc, c)
}

// Owns returns true if this Arena owns the buf.
func (s *Arena) Owns(buf []byte) bool {
	sc, c := s.bufContainer(buf)
	return sc != nil && c != nil
}

// GetNext returns the next chained buf for the given input buf.  The
// buf's managed by an Arena can be chained.  The returned bufNext may
// be nil.  When the returned bufNext is non-nil, the caller owns a
// ref-count on bufNext and must invoke DecRef(bufNext) when the
// caller is finished using bufNext.
func (s *Arena) GetNext(buf []byte) (bufNext []byte) {
	s.numGetNexts++
	sc, c := s.bufContainer(buf)
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
	return s.chunkMem(cNext)[0:c.next.chunkSize]
}

// SetNext associates the next chain buf following the input buf to be
// bufNext.  The buf's from an Arena can be chained, where buf will
// own an AddRef() on bufNext.  When buf's ref-count goes to zero, it
// will call DecRef() on bufNext.  The bufNext may be nil.
func (s *Arena) SetNext(buf, bufNext []byte) {
	s.numSetNexts++
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	if c.refs <= 0 {
		panic(fmt.Sprintf("unexpected ref-count during SetNext: %#v", c))
	}
	scOldNext, cOldNext := s.chunk(c.next)
	if scOldNext != nil && cOldNext != nil {
		s.decRef(scOldNext, cOldNext)
	}
	c.next = empty_chunkLoc
	if bufNext != nil {
		scNewNext, cNewNext := s.bufContainer(bufNext)
		if scNewNext == nil || cNewNext == nil {
			panic("bufNext not from this arena")
		}
		cNewNext.addRef()
		c.next = cNewNext.self
		c.next.chunkSize = len(bufNext)
	}
}

func (s *Arena) addSlabClass(chunkSize int) {
	s.slabClasses = append(s.slabClasses, slabClass{
		chunkSize: chunkSize,
		chunkFree: empty_chunkLoc,
	})
}

func (s *Arena) findSlabClassIndex(bufSize int) int {
	i := sort.Search(len(s.slabClasses),
		func(i int) bool { return bufSize <= s.slabClasses[i].chunkSize })
	if i >= len(s.slabClasses) {
		slabClass := &(s.slabClasses[len(s.slabClasses)-1])
		nextChunkSize := float64(slabClass.chunkSize) * s.growthFactor
		s.addSlabClass(int(math.Ceil(nextChunkSize)))
		return s.findSlabClassIndex(bufSize)
	}
	return i
}

func (s *Arena) assignChunkMem(slabClassIndex int) (chunkMem []byte) {
	sc := &(s.slabClasses[slabClassIndex])
	if sc.chunkFree.isEmpty() {
		if !s.addSlab(slabClassIndex, s.slabSize, s.slabMagic) {
			return nil
		}
	}
	return sc.chunkMem(sc.popFreeChunk())
}

func defaultMalloc(size int) []byte {
	return make([]byte, size)
}

func (s *Arena) addSlab(slabClassIndex, slabSize int, slabMagic int32) bool {
	sc := &(s.slabClasses[slabClassIndex])
	chunksPerSlab := slabSize / sc.chunkSize
	if chunksPerSlab <= 0 {
		chunksPerSlab = 1
	}
	slabIndex := len(sc.slabs)
	// Re-multiplying to avoid any extra fractional chunk memory.
	memorySize := (sc.chunkSize * chunksPerSlab) + slab_memory_footer_len
	s.numMallocs++
	memory := s.malloc(memorySize)
	if memory == nil {
		s.numMallocErrs++
		return false
	}
	slab := &slab{
		memory: memory,
		chunks: make([]chunk, chunksPerSlab),
	}
	footer := slab.memory[len(slab.memory)-slab_memory_footer_len:]
	binary.BigEndian.PutUint32(footer[0:4], uint32(slabClassIndex))
	binary.BigEndian.PutUint32(footer[4:8], uint32(slabIndex))
	binary.BigEndian.PutUint32(footer[8:12], uint32(slabMagic))
	sc.slabs = append(sc.slabs, slab)
	for i := 0; i < len(slab.chunks); i++ {
		c := &(slab.chunks[i])
		c.self.slabClassIndex = slabClassIndex
		c.self.slabIndex = slabIndex
		c.self.chunkIndex = i
		c.self.chunkSize = sc.chunkSize
		sc.pushFreeChunk(c)
	}
	sc.numChunks += int64(len(slab.chunks))
	return true
}

func (sc *slabClass) pushFreeChunk(c *chunk) {
	if c.refs != 0 {
		panic(fmt.Sprintf("pushFreeChunk() when non-zero refs: %v", c.refs))
	}
	c.next = sc.chunkFree
	sc.chunkFree = c.self
	sc.numChunksFree++
}

func (sc *slabClass) popFreeChunk() *chunk {
	if sc.chunkFree.isEmpty() {
		panic("popFreeChunk() when chunkFree is empty")
	}
	c := sc.chunk(sc.chunkFree)
	if c.refs != 0 {
		panic(fmt.Sprintf("popFreeChunk() when non-zero refs: %v", c.refs))
	}
	c.refs = 1
	sc.chunkFree = c.next
	c.next = empty_chunkLoc
	sc.numChunksFree--
	if sc.numChunksFree < 0 {
		panic("popFreeChunk() got < 0 numChunksFree")
	}
	return c
}

func (sc *slabClass) chunkMem(c *chunk) []byte {
	if c == nil || c.self.isEmpty() {
		return nil
	}
	beg := sc.chunkSize * c.self.chunkIndex
	return sc.slabs[c.self.slabIndex].memory[beg : beg+sc.chunkSize]
}

func (sc *slabClass) chunk(cl chunkLoc) *chunk {
	if cl.isEmpty() {
		return nil
	}
	return &(sc.slabs[cl.slabIndex].chunks[cl.chunkIndex])
}

func (s *Arena) chunkMem(c *chunk) []byte {
	if c == nil || c.self.isEmpty() {
		return nil
	}
	return s.slabClasses[c.self.slabClassIndex].chunkMem(c)
}

func (s *Arena) chunk(cl chunkLoc) (*slabClass, *chunk) {
	if cl.isEmpty() {
		return nil, nil
	}
	sc := &(s.slabClasses[cl.slabClassIndex])
	return sc, sc.chunk(cl)
}

// Determine the slabClass & chunk for a buf []byte.
func (s *Arena) bufContainer(buf []byte) (*slabClass, *chunk) {
	if buf == nil || cap(buf) <= slab_memory_footer_len {
		return nil, nil
	}
	rest := buf[:cap(buf)]
	footerDistance := len(rest) - slab_memory_footer_len
	footer := rest[footerDistance:]
	slabClassIndex := binary.BigEndian.Uint32(footer[0:4])
	slabIndex := binary.BigEndian.Uint32(footer[4:8])
	slabMagic := binary.BigEndian.Uint32(footer[8:12])
	if slabMagic != uint32(s.slabMagic) {
		return nil, nil
	}
	sc := &(s.slabClasses[slabClassIndex])
	slab := sc.slabs[slabIndex]
	chunkIndex := len(slab.chunks) - (footerDistance / sc.chunkSize)
	return sc, &(slab.chunks[chunkIndex])
}

func (c *chunk) addRef() *chunk {
	c.refs++
	if c.refs <= 1 {
		panic(fmt.Sprintf("unexpected ref-count during addRef: %#v", c))
	}
	return c
}

func (s *Arena) decRef(sc *slabClass, c *chunk) bool {
	c.refs--
	if c.refs < 0 {
		panic(fmt.Sprintf("unexpected ref-count during decRef: %#v", c))
	}
	if c.refs == 0 {
		scNext, cNext := s.chunk(c.next)
		if scNext != nil && cNext != nil {
			s.decRef(scNext, cNext)
		}
		c.next = empty_chunkLoc
		sc.pushFreeChunk(c)
		return true
	}
	return false
}

// Stats fills an input map with runtime metrics about the Arena.
func (s *Arena) Stats(m map[string]int64) map[string]int64 {
	m["numSlabClasses"] = int64(len(s.slabClasses))
	m["numAllocs"] = s.numAllocs
	m["numAddRefs"] = s.numAddRefs
	m["numDecRefs"] = s.numDecRefs
	m["numGetNexts"] = s.numGetNexts
	m["numSetNexts"] = s.numSetNexts
	m["numMallocs"] = s.numMallocs
	m["numMallocErrs"] = s.numMallocErrs
	m["numTooBigErrs"] = s.numTooBigErrs
	m["numNoChunkMemErrs"] = s.numNoChunkMemErrs
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
