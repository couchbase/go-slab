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
)

type Arena struct {
	growthFactor float64
	slabClasses  []slabClass // The chunkSizes of slabClasses grows by growthFactor.
	slabMagic    int32       // Magic number at the end of each slab memory []byte.
	slabSize     int
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
	next chunkLoc // Used when the chunk is in the free-list.
}

// Returns an Arena based on a slab allocator implementation.
// The startChunkSize and slabSize should be > 0.
// The growthFactor should be > 1.0.
func NewArena(startChunkSize int, slabSize int, growthFactor float64) *Arena {
	s := &Arena{
		growthFactor: growthFactor,
		slabMagic:    rand.Int31(),
		slabSize:     slabSize,
	}
	s.addSlabClass(startChunkSize)
	return s
}

// The input buf must be a buf returned by Alloc().  Once
// the buf's ref-count drops to 0, the Arena may re-use the buf.
func (s *Arena) Alloc(bufSize int) (buf []byte) {
	if bufSize > s.slabSize {
		return nil
	}
	return s.assignChunkMem(s.findSlabClassIndex(bufSize))[0:bufSize]
}

// The input buf must be a buf returned by Alloc().
func (s *Arena) AddRef(buf []byte) {
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	c.addRef()
}

// The buf must be from an Alloc() from the same Arena.
func (s *Arena) DecRef(buf []byte) {
	sc, c := s.bufContainer(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	sc.decRef(c)
}

// Returns true if this Arena owns the buf.
func (s *Arena) Owns(buf []byte) bool {
	sc, c := s.bufContainer(buf)
	return sc != nil && c != nil
}

func (s *Arena) addSlabClass(chunkSize int) {
	s.slabClasses = append(s.slabClasses, slabClass{
		chunkSize: chunkSize,
		chunkFree: empty_chunkLoc,
	})
}

func (s *Arena) findSlabClassIndex(bufSize int) int {
	curr := 0
	for {
		// TODO: Use binary search instead of linear walk.
		slabClass := &(s.slabClasses[curr])
		if bufSize <= slabClass.chunkSize {
			return curr
		}
		if curr+1 >= len(s.slabClasses) {
			nextChunkSize := float64(slabClass.chunkSize) * s.growthFactor
			s.addSlabClass(int(math.Ceil(nextChunkSize)))
		}
		curr++
	}
}

func (s *Arena) assignChunkMem(slabClassIndex int) (chunkMem []byte) {
	sc := &(s.slabClasses[slabClassIndex])
	if sc.chunkFree.isEmpty() {
		sc.addSlab(slabClassIndex, sc.chunkSize, s.slabSize, s.slabMagic)
	}
	return sc.chunkMem(sc.popFreeChunk())
}

func (sc *slabClass) addSlab(slabClassIndex, chunkSize, slabSize int, slabMagic int32) {
	chunksPerSlab := slabSize / chunkSize
	slabIndex := len(sc.slabs)
	slab := &slab{
		// Re-multiplying to avoid any extra fractional chunk memory.
		memory: make([]byte, (chunkSize*chunksPerSlab)+SLAB_MEMORY_FOOTER_LEN),
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

func (sc *slabClass) decRef(c *chunk) *chunk {
	c.refs--
	if c.refs < 0 {
		panic(fmt.Sprintf("unexpected ref-count during decRef: %#v", c))
	}
	if c.refs == 0 {
		sc.pushFreeChunk(c)
	}
	return c
}
