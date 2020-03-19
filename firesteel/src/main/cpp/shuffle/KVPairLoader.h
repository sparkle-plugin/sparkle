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

  inline uint64_t getTotalNumKVPairs() {
    return size;
  }

  inline uint64_t getDataChunkBytesRead() {
    return bytesRead;
  }

  inline uint64_t getDataChunksRead() {
    return numChunksRead;
  }

  inline uint64_t getRemoteDataChunkBytesRead() {
    return remoteBytesRead;
  }

  inline uint64_t getRemoteDataChunksRead() {
    return numRemoteChunksRead;
  }

protected:
  const int reducerId;
  vector<pair<region_id, offset>>& chunkPtrs; //index chunk pointers.
  chunk mergedChunk;
  uint64_t size {0}; // # of kv pairs in whole chunks.
  byte* dropUntil(int partitionId, byte* indexChunkPtr);
private:
  uint64_t bytesRead {0}; // the size of the whole local data chunks read.
  uint64_t numChunksRead {0}; // # of the local data chunks read.
  uint64_t remoteBytesRead {0}; // the size of the whole remote data chunks read.
  uint64_t numRemoteChunksRead {0}; // # of the remote data chunks read.

  /**
   * load the whole chunks in memory as kv pairs.
   */
  size_t load(int reducerId);

  inline void incBytesRead(uint64_t bytes) {
    bytesRead += bytes;
  }

  inline void incChunksRead(uint64_t numChunks) {
    numChunksRead += numChunks;
  }

  inline void incRemoteBytesRead(uint64_t bytes) {
    remoteBytesRead += bytes;
  }

  inline void incRemoteChunksRead(uint64_t numChunks) {
    numRemoteChunksRead += numChunks;
  }
};

class PassThroughLoader : public KVPairLoader {
public:
  PassThroughLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {
    itFlatChunk = mergedChunk.begin();
    curSize = size;
  }
  ~PassThroughLoader() {}

  void prepare(JNIEnv* env) override;
  vector<ReduceKVPair> fetch(int num) override;
private:
  chunk::iterator itFlatChunk;
  size_t curSize;
};

class HashMapLoader : public KVPairLoader {
public:
  HashMapLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {}
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

    bool operator()(const pair<int, jobject>& lhs, const pair<int, jobject>& rhs) const {
      if (lhs.first != rhs.first) {
        return false;
      }

      jclass clazz {env->GetObjectClass(lhs.second)};
      jmethodID equals
        {env->GetMethodID(clazz, "equals", "(Ljava/lang/Object;)Z")};
      return env->CallBooleanMethod(lhs.second, equals, rhs.second);
    }

  private:
    JNIEnv* env {nullptr};
  };

  struct Hasher {
  public:
    Hasher() {}
    ~Hasher() {}

    uint64_t operator()(const pair<int, jobject>& key) const {
      return key.first;
    }
  };

  unique_ptr<unordered_map<pair<int, jobject>, vector<ReduceKVPair>, Hasher, EqualTo>> hashmap;
  unordered_map<pair<int, jobject>, vector<ReduceKVPair>, Hasher, EqualTo>::iterator hashmapIt;
};

class MergeSortLoader : public KVPairLoader {
public:
  MergeSortLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {
    curSize = size;
  }
  ~MergeSortLoader() {}

  void prepare(JNIEnv* env) override;
  vector<ReduceKVPair> fetch(int num) override;
private:
  void order(JNIEnv* env);
  chunk::iterator itOrderedChunk;
  size_t curSize;
};
#endif
