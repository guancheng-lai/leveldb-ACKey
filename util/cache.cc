// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }
  void AdjustCapacity(int capacity) { capacity_ += capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));

  // Like Cache methods, but with extra "hash" and "ghostCache" parameters.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value, size_t charge, Cache* ghost,
                        void (*deleter)(const Slice &, void*));

  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_ GUARDED_BY(mutex_);

  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value, size_t charge, Cache* ghost,
                                 void (*deleter)(const Slice&, void*)) {
  MutexLock l(&mutex_);

  LRUHandle* e =
    reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    auto* hd = ghost->Insert(Slice(old->key_data, old->key_length), new int(old->charge), 1,
     [](const Slice& key, void* value) { delete reinterpret_cast<int*>(value); }
    );
    ghost->Release(hd);
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
  size_t capacity_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0), capacity_(capacity) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* InsertARC(const Slice& key, void* value, size_t charge, Cache* ghost,
                    void (*deleter)(const Slice&, void*)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, ghost, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
  void AdjustCapacity(int adjustment) override {
    if (adjustment < 0 && capacity_ < 8 << 18) return;
    for (auto& lru : shard_) {
      lru.AdjustCapacity(adjustment / kNumShards);
    }
    capacity_ += adjustment;
  }
  size_t GetCapacity() const override {
    return capacity_;
  }
};

}  // end anonymous namespace

// AdaptiveCache
Cache::Handle* AdaptiveCache::Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*)) {
  assert(ghost);
  return real->InsertARC(key, value, charge, this->ghost, deleter);
}
Cache::Handle* AdaptiveCache::Lookup(const Slice& key, int& ghostHit) {
  Cache::Handle* handle = real->Lookup(key);
  if (handle != nullptr) {
    return handle;
  }

  handle = ghost->Lookup(key);
  if (handle != nullptr) {
    ghostHit = *reinterpret_cast<int*>(ghost->Value(handle));
    ghost->Release(handle);
  }
  return nullptr;
}
void AdaptiveCache::Release(Cache::Handle* handle) {
  real->Release(handle);
}
void* AdaptiveCache::Value(Cache::Handle* handle) {
  return real->Value(handle);
}
uint64_t AdaptiveCache::NewId() {
  return real->NewId();
}
size_t AdaptiveCache::TotalCharge() const {
  return real->TotalCharge() + ghost->TotalCharge();
}
size_t AdaptiveCache::TotalRealCharge() const {
  return real->TotalCharge();
}
size_t AdaptiveCache::TotalGhostCharge() const {
  return ghost->TotalCharge();
}
void AdaptiveCache::AdjustCapacity(int adjustment) {
  MutexLock l(&mutex_);
  accumulateAdjustment += adjustment;
  if (accumulateAdjustment > 4096 || accumulateAdjustment < -4096) {
    accumulateAdjustment = 0;
    double ratio = static_cast<double>(ghost->TotalCharge()) / static_cast<double>(real->TotalCharge());
    ghost->AdjustCapacity(static_cast<int>(adjustment * ratio / (ratio + 1.0)));
    real->AdjustCapacity(static_cast<int>(adjustment / (ratio + 1.0)));
  }
}
size_t AdaptiveCache::GetCapacity() const {
  return real->GetCapacity();
}
Cache::Handle* AdaptiveCache::Lookup(const Slice& key) {
  assert(false);
  return nullptr;
}
void AdaptiveCache::Erase(const Slice& key) {
  assert(false);
}
void AdaptiveCache::Prune() {
  assert(false);
}
Cache* AdaptiveCache::realCache() const {
  return real;
}
Cache* AdaptiveCache::ghostCache() const {
  return ghost;
}
// AdaptiveCache

// BlockCache
Cache::Handle* BlockCache::Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*)) {
  return bk->Insert(key, value, charge, deleter);
}
Cache::Handle* BlockCache::Lookup(const Slice& key, int& ghostHit) {
  return bk->Lookup(key, ghostHit);
}
void* BlockCache::Value(Cache::Handle* handle) {
  return bk->Value(handle);
}
uint64_t BlockCache::NewId() {
  return bk->NewId();
}
size_t BlockCache::TotalCharge() const {
  return bk->TotalCharge();
}
size_t BlockCache::TotalRealCharge() const {
  return bk->TotalRealCharge();
}
size_t BlockCache::TotalGhostCharge() const {
  return bk->TotalGhostCharge();
}
void BlockCache::AdjustCapacity(int adjustment) {
  bk->AdjustCapacity(adjustment);
}
size_t BlockCache::GetCapacity() const {
  return bk->GetCapacity();
}
Cache::Handle *BlockCache::Lookup(const Slice &key) {
  return bk->Lookup(key);
}
void BlockCache::Release(Cache::Handle *handle) {
  bk->Release(handle);
}
void BlockCache::Erase(const Slice &key) {
  bk->Erase(key);
}
// BlockCache

// PointCache
Cache::Handle* PointCache::InsertKV(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*)) {
  return kv->Insert(key, value, charge, deleter);
}
Cache::Handle* PointCache::InsertKP(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*)) {
  return kp->Insert(key, value, charge, deleter);
}
size_t PointCache::TotalCharge() const {
  return kv->TotalCharge() + kp->TotalCharge();
}
size_t PointCache::TotalKVCharge() const {
  return kv->TotalCharge();
}
size_t PointCache::TotalKPCharge() const {
  return kp->TotalCharge();
}
Cache::Handle* PointCache::LookupKV(const Slice& key, int& ghostHit) {
  return kv->Lookup(key, ghostHit);
}
Cache::Handle* PointCache::LookupKP(const Slice& key, int& ghostHit) {
  return kp->Lookup(key, ghostHit);
}
void* PointCache::ValueKV(Cache::Handle* handle) {
  return kv->Value(handle);
}
void* PointCache::ValueKP(Cache::Handle* handle) {
  return kp->Value(handle);
}
void PointCache::ReleaseKV(Cache::Handle* handle) {
  kv->Release(handle);
}
void PointCache::ReleaseKP(Cache::Handle* handle) {
  kp->Release(handle);
}
void PointCache::AdjustCapacity(int adjustment) {
  double ratio = static_cast<double>(TotalKVCharge()) / static_cast<double>(TotalKPCharge());
  kv->AdjustCapacity(adjustment * (ratio / (1.0 + ratio)));
  kp->AdjustCapacity(adjustment / (1.0 + ratio));
}
void PointCache::AdjustKVCapacity(int adjustment) {
  kv->AdjustCapacity(adjustment);
}
void PointCache::AdjustKPCapacity(int adjustment) {
  kp->AdjustCapacity(adjustment);
}
AdaptiveCache* PointCache::KVCache() const {
  return kv;
}
AdaptiveCache* PointCache::KPCache() const {
  return kp;
}
size_t PointCache::GetKVCapacity() const {
  return kv->GetCapacity();
}
size_t PointCache::GetKPCapacity() const {
  return kp->GetCapacity();
}
// PointCache

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }
BlockCache* NewBlockCache(size_t capacity) { return new BlockCache(capacity); }
PointCache* NewPointCache(size_t capacity) { return new PointCache(capacity); }

}  // namespace leveldb
