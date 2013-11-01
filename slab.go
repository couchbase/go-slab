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

package slab

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

type Arena struct {
	growthFactor float64
	slabClasses  []slabClass // The chunkSizes of slabClasses grows by growthFactor.
	slabMagic    int32       // Magic number at the end of each slab memory []byte.
	slabSize     int
	malloc       func(size int) []byte // App-specific allocator; may be nil.
}

type slabClass struct {
	slabs     []*slab  // A growing array of slabs.
	chunkSize int      // Each slab is sliced into fixed-sized chunks.
	chunkFree chunkLoc // Chunks are tracked in a free-list per slabClass.
}

type slab struct {
	memory []byte  // len(memory) == slabSize + SLAB_MEMORY_FOOTER_LEN.
	chunks []chunk // Parallel array of chunk metadata.
}

const SLAB_MEMORY_FOOTER_LEN int = 4 + 4 + 4 // slabClassIndex + slabIndex + slabMagic.

type chunkLoc struct {
	slabClassIndex int
	slabIndex      int
	chunkIndex     int
}

var empty_chunkLoc = chunkLoc{-1, -1, -1} // A sentinel.

func (cl *chunkLoc) isEmpty() bool {
	return cl.slabClassIndex == -1 && cl.slabIndex == -1 && cl.chunkIndex == -1
}

type chunk struct {
	refs int32    // Ref-count.
	self chunkLoc // The self is the chunkLoc for this chunk.
	next chunkLoc // Used when the chunk is in the free-list or when chained.
}

// Returns an Arena based on a slab allocator implementation.
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

// Alloc() may return nil on errors, such as if no more free chunks
// are available and new slab memory was not allocatable (such as if
// malloc() returns nil).
func (s *Arena) Alloc(bufSize int) (buf []byte) {
	if bufSize > s.slabSize {
		return nil
	}
	chunkMem := s.assignChunkMem(s.findSlabClassIndex(bufSize))
	if chunkMem == nil {
		return nil
	}
	return chunkMem[0:bufSize]
}

// The input buf must be from an Alloc() from the same Arena.
func (s *Arena) AddRef(buf []byte) {
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	c.addRef()
}

// The input buf must be from an Alloc() from the same Arena.  Once
// the buf's ref-count drops to 0, the Arena may re-use the buf.
func (s *Arena) DecRef(buf []byte) {
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	s.decRef(sc, c)
}

// Returns true if this Arena owns the buf.
func (s *Arena) Owns(buf []byte) bool {
	sc, c := s.bufContainer(buf)
	return sc != nil && c != nil
}

// The buf's from an Arena can be chained.  The returned bufNext may
// be nil.  When the returned bufNext is non-nil, the caller owns a
// ref-count on bufNext and must invoke DecRef(bufNext) when the
// caller is finished using bufNext.
func (s *Arena) GetNext(buf []byte) (bufNext []byte) {
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
	return s.chunkMem(cNext)
}

// The buf's from an Arena can be chained, where buf will own an
// AddRef() on bufNext.  When buf's ref-count goes to zero, it will
// call DecRef() on bufNext.  The bufNext may be nil.
func (s *Arena) SetNext(buf, bufNext []byte) {
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
	memorySize := (sc.chunkSize * chunksPerSlab) + SLAB_MEMORY_FOOTER_LEN
	memory := s.malloc(memorySize)
	if memory == nil {
		return false
	}
	slab := &slab{
		memory: memory,
		chunks: make([]chunk, chunksPerSlab),
	}
	footer := slab.memory[len(slab.memory)-SLAB_MEMORY_FOOTER_LEN:]
	binary.BigEndian.PutUint32(footer[0:4], uint32(slabClassIndex))
	binary.BigEndian.PutUint32(footer[4:8], uint32(slabIndex))
	binary.BigEndian.PutUint32(footer[8:12], uint32(slabMagic))
	sc.slabs = append(sc.slabs, slab)
	for i := 0; i < len(slab.chunks); i++ {
		c := &(slab.chunks[i])
		c.self.slabClassIndex = slabClassIndex
		c.self.slabIndex = slabIndex
		c.self.chunkIndex = i
		sc.pushFreeChunk(c)
	}
	return true
}

func (sc *slabClass) pushFreeChunk(c *chunk) {
	if c.refs != 0 {
		panic(fmt.Sprintf("pushFreeChunk() when non-zero refs: %v", c.refs))
	}
	c.next = sc.chunkFree
	sc.chunkFree = c.self
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
	if buf == nil || cap(buf) <= SLAB_MEMORY_FOOTER_LEN {
		return nil, nil
	}
	rest := buf[:cap(buf)]
	footerDistance := len(rest) - SLAB_MEMORY_FOOTER_LEN
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

func (s *Arena) decRef(sc *slabClass, c *chunk) *chunk {
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
	}
	return c
}
