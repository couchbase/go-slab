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

// Generic interface for []byte memory management.
type Arena interface {
	// The returned buf should not be sliced larger than bufSize.
	// Callers must DecRef(buf) when they're done with it.
	Alloc(bufSize int) (buf []byte)

	// The input buf must be a buf returned by Alloc().
	AddRef(buf []byte)

	// The input buf must be a buf returned by Alloc().  Once
	// the buf's ref-count drops to 0, the Arena may re-use the buf.
	DecRef(buf []byte)
}
