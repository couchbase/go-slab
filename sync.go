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
	"sync"
)

// Wraps an Arena with sync.Mutex protection.
func SynchronizedArena(arena Arena) Arena {
	return &synchronizedArena{arena: arena}
}

type synchronizedArena struct {
	lock  sync.Mutex
	arena Arena
}

func (s *synchronizedArena) Alloc(bufSize int) (buf []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.arena.Alloc(bufSize)
}

func (s *synchronizedArena) AddRef(buf []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.arena.AddRef(buf)
}

func (s *synchronizedArena) DecRef(buf []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.arena.DecRef(buf)
}
