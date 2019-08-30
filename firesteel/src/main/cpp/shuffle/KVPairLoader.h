#ifndef __KVPAIRLOADER_H_
#define __KVPAIRLOADER_H_

#include <jni.h>
#include <vector>
#include <unordered_map>
#include <utility>
#include <cstdint>
#include <stdexcept>
#include <chrono>
#include <glog/logging.h>
#include "KVPair.h"
//#include "../jnishuffle/JniUtils.h"

using namespace std;

typedef uint64_t region_id;
typedef uint64_t offset;
typedef uint64_t chunk_id;
typedef vector<ReduceKVPair> chunk;

class KVPairLoader {
public:
  KVPairLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : reducerId(_reducerId), chunkPtrs(_chunkPtrs) {
    auto start = chrono::system_clock::now();
    size = load(reducerId);
    auto end = chrono::system_clock::now();
    chrono::duration<double> elapsed_s = end - start;
    LOG(INFO) << "load " << size << " pairs from shm took " << elapsed_s.count() << "s";
  }
  virtual ~KVPairLoader() {}

  virtual void prepare(JNIEnv* env) =0;

  /**
   * fetch the number of kv pairs.
   * Note: we should call `load` before this method.
   */
  virtual vector<ReduceKVPair> fetch(int num) {
    throw new domain_error("not valid here.");
  }
  virtual vector<vector<ReduceKVPair>> fetchAggregatedPairs(int num) {
    throw new domain_error("not valid here.");
  }

protected:
  const int reducerId;
  vector<pair<region_id, offset>>& chunkPtrs; //index chunk pointers.
  vector<pair<chunk_id, chunk>> dataChunks;
  uint64_t size {0};
  byte* dropUntil(int partitionId, byte* indexChunkPtr);
private:
  /**
   * load the whole chunks in memory as kv pairs.
   */
  size_t load(int reducerId);
};

class PassThroughLoader : public KVPairLoader {
public:
  PassThroughLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {
    flatChunk.reserve(size);
    itFlatChunk = flatChunk.begin();
  }
  ~PassThroughLoader() {}

  void prepare(JNIEnv* env) override;
  vector<ReduceKVPair> fetch(int num) override;
private:
  vector<ReduceKVPair> flatChunk;
  vector<ReduceKVPair>::iterator itFlatChunk;
  void flatten();
};

class HashMapLoader : public KVPairLoader {
public:
  HashMapLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {
  }
  ~HashMapLoader() {}

  void prepare(JNIEnv* env) override;
  vector<vector<ReduceKVPair>> fetchAggregatedPairs(int num) override;
private:
  void aggregate(JNIEnv* env);

  // implement Equal-related functors here.
  struct EqualTo {
  public:
    EqualTo(JNIEnv* env) : env(env) {}
    ~EqualTo() {}

    bool operator()(const jobject& lhs, const jobject& rhs) const {
      jclass clazz {env->GetObjectClass(lhs)};
      jmethodID equals
        {env->GetMethodID(clazz, "equals", "(Ljava/lang/Object;)Z")};
      return env->CallBooleanMethod(lhs, equals, rhs);
    }

  private:
    JNIEnv* env {nullptr};
  };

  struct Hasher {
  public:
    Hasher(JNIEnv* env) : env(env) {}
    ~Hasher() {}

    uint64_t operator()(const jobject& key) const {
      jclass clazz {env->GetObjectClass(key)};
      jmethodID hasher
        {env->GetMethodID(clazz, "hashCode", "()I")};
      return (uint64_t) env->CallIntMethod(key, hasher);
    }

  private:
    JNIEnv* env {nullptr};
  };

  unique_ptr<unordered_map<jobject, vector<ReduceKVPair>, Hasher, EqualTo>> hashmap;
};

class MergeSortLoader : public KVPairLoader {
public:
  MergeSortLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {
    orderedChunk.reserve(size);
    itOrderedChunk = orderedChunk.begin();
  }
  ~MergeSortLoader() {}

  void prepare(JNIEnv* env) override;
  vector<ReduceKVPair> fetch(int num) override;
private:
  void order(JNIEnv* env);
  vector<ReduceKVPair> orderedChunk;
  vector<ReduceKVPair>::iterator itOrderedChunk;
};
#endif
