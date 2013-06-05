# go-slab - slab allocator in go

A slab allocator library in the Go Programming Language.

# Who is this for

This library may be interestng to you if you wish to reduce garbage
collection (e.g. stop-the-world GC) performance issues in your golang
programs, allowing you to switch to explicit byte array memory
management techniques.

This can be useful, for example, for long-running server programs that
manage lots of in-memory data items, such as caches and databases.

# Example usage

    arena := NewArena(48,         // The smallest slab class "chunk size" is 48 bytes.
                      1024*1024,  // Each slab will be 1MB in size.
                      2)          // Power of 2 growth in "chunk sizes".

    var buf []byte

    buf = arena.Alloc(64)         // Allocate 64 bytes.
      ... use the buf ...
    arena.DecRef(buf)             // Release our ref-count when we're done with buf.

    buf = arena.Alloc(1024)       // Allocate another 1K byte array.
      ... use the buf ...
    arena.AddRef(buf)             // The buf's ref-count now goes to 2.
      ... use the buf some more ...
    arena.DecRef(buf)
      ... still can use the buf since we still have 1 ref-count ...
    arena.DecRef(buf)             // We shouldn't use the buf after this last DecRef(),
                                  // as the library might recycle it for a future Alloc().

# Design concepts

The byte arrays ([]byte) that are allocated by this library are
reference-counted.  When a byte array's reference count drops to 0, it
will be placed onto a free-list for later re-use.  This can reduce the
need to ask the go runtime to allocate new memory and perhaps delay
the need for a full stop-the-world GC.

The AddRef()/DecRef() functions use slice/capacity math instead of
"large" additional tracking data structures (e.g., no extra
hashtables) in order to reach the right ref-counter metadata.

This implementation also does not use any of go's "unsafe"
capabilities, allowing it to remain relatively simple.

Memory is managed via a simple slab allocator algorithm.  See:
http://en.wikipedia.org/wiki/Slab_allocation

Each arena tracks one or more slabClass structs.  Each slabClass
manages a different "chunk size", where chunk sizes are computed using
a simple "growth factor" (e.g., the "power of 2 growth" in the above
example).  Each slabClass also tracks a zero or more slab structs,
where every slab in a slabClass will all have the same chunk size.  A
slab manages a (usually large) continguous array of memory bytes (1MB
from the above example), and the slab's memory is subdivided into many
fixed-sized chunks of the same chunk size.  All the chunks in a new
slab are placed on a free-list that's part of the slabClass.

When Alloc() is invoked, the first "large enough" slabClass is found,
and a chunk from the free-list is taken to service the allocation.  If
there are no more free chunks available in a slabClass, then a new
slab (e.g., 1MB) is allocated, chunk'ified, and the request is
processed as before.

# Concurrency

The Arena returned from NewArena() is not concurrency safe.
Please use your own locking.

# Rules

* You need to invoke AddRef()/DecRef() with the exact same buf
  that you received from Alloc(), from the same arena.
* Don't call Alloc() with a size greater than the arena's slab size.
  e.g., if your slab size is 1MB, then Alloc(1024 * 1024 + 1) will fail.
* Careful with your ref-counting -- that's the fundamental tradeoff
  with now trying to avoid GC.

# LICENSE

Apache 2 license.

# Testing

Unit test code coverage, as of 0.0.0-0-gecfea2f, is 100%.

# TODO

* Use a binary search to find a slabClass faster.
* Currently, slabs that are allocated are never freed.
* Memory for one slabClass is never reassigned to another slabClass.
  Memory reassignment might be useful whenever data sizes of items in
  long-running systems change over time.  For example, sessions in an
  online game may initially fit fine into a 1K slab class, but start
  getting larger than 1K as players acquire more inventory.
  Meanwhile, most of the slab memory is "stuck" in the 1K slab class
  when it's now needed in the 2K slab class.
