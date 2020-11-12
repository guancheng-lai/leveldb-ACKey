// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

  struct LRUHandle {
    void* value;
    void (*deleter)(const Slice&, void* value);
    LRUHandle* next_hash;
    LRUHandle* next;
    LRUHandle* prev;
    size_t charge;  // TODO(opt): Only allow uint32_t?
    size_t key_length;
    bool in_cache;     // Whether entry is in the cache.
    uint32_t refs;     // References, including cache reference, if present.
    uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
    char key_data[1];  // Beginning of key

    Slice key() const {
      // next_ is only equal to this if the LRU handle is the list head of an
      // empty list. List heads never have meaningful keys.
      assert(next != this);

      return Slice(key_data, key_length);
    }
  };

class LEVELDB_EXPORT Cache;
class LEVELDB_EXPORT BlockCache;
class LEVELDB_EXPORT PointCache;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);
LEVELDB_EXPORT BlockCache* NewBlockCache(size_t capacity);
LEVELDB_EXPORT PointCache* NewPointCache(size_t capacity);

class LEVELDB_EXPORT Cache {
 public:
  Cache() = default;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle {};

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;
  // Like Insert(), but it will move entries into ghost cache if usage exceed the capacity
  virtual Handle* InsertARC(const Slice& key, void* value, size_t charge, Cache* ghost,
                            void (*deleter)(const Slice&, void*)) { return Insert(key, value, charge, deleter); }

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;
  // Like Lookup(), but it will lookup entries by using shorten hash value
  virtual Handle* LookupGhost(const Slice& key) { return Lookup(key); };

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // leveldb may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  virtual size_t TotalCharge() const = 0;

  // Adjust cache capacity, it could be either expanding or shrinking
  virtual void AdjustCapacity(size_t capacity) = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};

class LEVELDB_EXPORT AdaptiveCache : public Cache {
 private:
  Cache *real, *ghost;
 public:
  AdaptiveCache() = delete;
  explicit AdaptiveCache(size_t capacity) : real(NewLRUCache(capacity/2)), ghost(NewLRUCache(capacity/2)) {}
  ~AdaptiveCache() { delete real, delete ghost; }
  Cache::Handle* Lookup(const Slice& key, int &ghostHit);
  Cache::Handle* Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*)) override;
  void Release(Cache::Handle* handle) override;
  void* Value(Cache::Handle* handle) override;
  Handle* Lookup(const Slice& key) override;
  void Erase(const Slice& key) override;
  void Prune() override;
  uint64_t NewId() override;
  size_t TotalCharge() const override;
  size_t TotalRealCharge() const;
  size_t TotalGhostCharge() const;
  void AdjustCapacity(size_t size) override;
  Cache* realCache() const;
  Cache* ghostCache() const;
};

class LEVELDB_EXPORT BlockCache : public Cache {
 private:
  AdaptiveCache* bk;
  size_t adjustment;
 public:
  explicit BlockCache(size_t capacity) : bk{new AdaptiveCache(capacity)}, adjustment(0) {}
  ~BlockCache() { delete bk; }
  Cache::Handle* Lookup(const Slice& key, int &ghostHit);
  Cache::Handle* Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*));
  Handle* Lookup(const Slice& key);
  void Release(Handle* handle);
  void Erase(const Slice& key);
  void* Value(Cache::Handle* handle);
  uint64_t NewId();
  size_t TotalCharge() const;
  size_t TotalRealCharge() const;
  size_t TotalGhostCharge() const;
  void AdjustCapacity(size_t adjust);
};

class LEVELDB_EXPORT PointCache {
 private:
  AdaptiveCache* kv;
  AdaptiveCache* kp;
 public:
  explicit PointCache(size_t capacity) : kv{new AdaptiveCache(capacity/2)}, kp{new AdaptiveCache(capacity/2)} {}
  ~PointCache() { delete kv, delete kp; }
  Cache::Handle* InsertKV(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*));
  Cache::Handle* InsertKP(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*));
  Cache::Handle* LookupKV(const Slice& key, int& ghostHit);
  Cache::Handle* LookupKP(const Slice& key, int& ghostHit);
  void* ValueKV(Cache::Handle* handle);
  void* ValueKP(Cache::Handle* handle);
  void ReleaseKV(Cache::Handle* handle);
  void ReleaseKP(Cache::Handle* handle);
  void AdjustPointCacheCapacity(size_t adjustment);
  void AdjustKVCapacity(size_t adjustment);
  void AdjustKPCapacity(size_t adjustment);
  size_t TotalCharge() const;
  size_t TotalKvCharge() const;
  size_t TotalKpCharge() const;
  AdaptiveCache* kvCache() const;
  AdaptiveCache* kpCache() const;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
