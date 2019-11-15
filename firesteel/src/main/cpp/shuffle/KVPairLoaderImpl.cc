#include <memory>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <iterator>
#include <utility>
#include <functional>
#include <cstring>
#include <chrono>
#include <glog/logging.h>
#include "globalheap/globalheap.hh"
#include "KVPairLoader.h"
#include "SimpleUtils.h"
#include "../jnishuffle/JniUtils.h"

using namespace std;
using namespace alps;

size_t
KVPairLoader::load(int reducerId) {
  // decode the index chunk.
  vector<pair<region_id, offset>> dataChunkPtrs;
  vector<int> numPairs;
  vector<bool> isLocalDataChunk;
  int currentNumaNode {OsUtil::getCurrentNumaNode()};
  size_t numLoadedPairs {0};
  for (auto& chunkPtr : chunkPtrs) {
    RRegion::TPtr<void> idxChunkPtr(chunkPtr.first, chunkPtr.second);
    byte* index = (byte*)idxChunkPtr.get();
    {
      // decode numa node identifier of the corresponding data chunk.
      int numa;
      memcpy(&numa, index, sizeof(int));
      index += sizeof(int);

      // discard key's type
      index += sizeof(int);

      // decode # of partitions in this map output.
      int numBuckets;
      memcpy(&numBuckets, index, sizeof(int));
      index +=  sizeof(int);

      // drop unnecessary chunks ptr considering for  # of partitions.
      if (reducerId > numBuckets-1) {
        continue;
      }
      index = dropUntil(reducerId, index);

      // decode data chunk ptr and its size.
      region_id rid;
      memcpy(&rid, index, sizeof(region_id));
      index += sizeof(region_id);

      offset roffset;
      memcpy(&roffset, index, sizeof(offset));
      index += sizeof(offset);

      int size; // in bytes.
      memcpy(&size, index, sizeof(int));
      index += sizeof(int);

      int numPair; // # of pairs in this chunk.
      memcpy(&numPair, index, sizeof(int));
      index += sizeof(int);

      if (size == 0) {
        continue; // skip empty data chunks.
      }
      isLocalDataChunk.emplace_back(numa == currentNumaNode);
      dataChunkPtrs.emplace_back(rid, roffset);
      numPairs.emplace_back(numPair);
      numLoadedPairs += numPair;
    }
  }

  // decode data chunks.
  mergedChunk.reserve(numLoadedPairs);
  for (size_t i=0; i<dataChunkPtrs.size(); ++i) {
    RRegion::TPtr<void> dataChunkPtr(dataChunkPtrs[i].first, dataChunkPtrs[i].second);
    byte* index = (byte*) dataChunkPtr.get();

    for (int j=0; j<numPairs[i]; ++j) {
      // [keyHash, serKeySize, serKey, serValueSize, serValue]
      {
        int keyHash;
        memcpy(&keyHash, index, sizeof(int));
        index += sizeof(int);

        int serKeySize;
        memcpy(&serKeySize, index, sizeof(int));
        index += sizeof(int);

        byte* serKey {new byte[serKeySize]};
        memcpy(serKey, index, serKeySize);
        index += serKeySize;

        int serValueSize;
        memcpy(&serValueSize, index, sizeof(int));
        index += sizeof(int);

        byte* serValue {new byte[serValueSize]};
        memcpy(serValue, index, serValueSize);
        index += serValueSize;

        mergedChunk.emplace_back(serKey, serKeySize, keyHash,
                                 serValue, serValueSize, reducerId);

        uint64_t sizeChunk {sizeof(int)*3 + serKeySize + serValueSize};
        isLocalDataChunk[i] ? incBytesRead(sizeChunk) : incRemoteBytesRead(sizeChunk);
      }
    }

    isLocalDataChunk[i] ? incChunksRead(1) : incRemoteChunksRead(1);
  }

  return mergedChunk.size();
}

byte*
KVPairLoader::dropUntil(int partitionId, byte* index) {
  for (int i=0; i<partitionId; ++i) {
    index += sizeof(region_id) + sizeof(offset) + sizeof(int)*2;
  }

  return index;
}

void
PassThroughLoader::prepare(JNIEnv* env) {}

vector<ReduceKVPair>
PassThroughLoader::fetch(int num) {
  auto start = chrono::system_clock::now();

  auto beginIt = itFlatChunk;
  auto endIt = num <= (int)curSize ? beginIt + num : mergedChunk.end();
  auto res = vector<ReduceKVPair>(beginIt, endIt);
  itFlatChunk = endIt;
  curSize -= num;

  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "fetch " << res.size() << " pairs from flatChunk took " << elapsed_s.count() << "s";

  return res;
}

void
HashMapLoader::prepare(JNIEnv* env) {
  auto start = chrono::system_clock::now();
  shuffle::deserializeKeys(env, mergedChunk);
  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "deserializing " << size << " pairs from chunks took " << elapsed_s.count() << "s";

  start = chrono::system_clock::now();
  aggregate(env);
  end = chrono::system_clock::now();
  elapsed_s = end - start;
  LOG(INFO) << "aggregating " << size << " pairs into the hashmap took " << elapsed_s.count() << "s";
}

void
HashMapLoader::aggregate(JNIEnv* env) {
  // group by keys.
  hashmap =
    make_unique<unordered_map<pair<int, jobject>, vector<ReduceKVPair>, Hasher, EqualTo>>(0, Hasher(), EqualTo(env));
  for (auto& pair : mergedChunk) {
    auto hashKey = make_pair(pair.getKeyHash(), pair.getKey());
    auto it {hashmap->find(hashKey)};
    if (it == hashmap->end()) {
      hashmap->emplace(hashKey, vector<ReduceKVPair>{pair});
      continue;
    }

    auto& pairs {it->second};
    pairs.emplace_back(move(pair));
  }

  hashmapIt = hashmap->begin();
}

vector<vector<ReduceKVPair>>
HashMapLoader::fetchAggregatedPairs(int num) {
  auto start = chrono::system_clock::now();

  vector<vector<ReduceKVPair>> res;
  auto beginIt = hashmapIt;
  auto endIt = next(beginIt,
                    min(num, static_cast<int>(distance(beginIt, hashmap->end()))));

  for (auto it=beginIt; it!=endIt; ++it) {
    res.emplace_back(move(it->second));
  }

  hashmapIt = endIt;

  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "fetch " << res.size() << " pairs from chunks took " << elapsed_s.count() << "s";

  return res;
}


void
MergeSortLoader::prepare(JNIEnv* env) {
  auto start = chrono::system_clock::now();
  shuffle::deserializeKeys(env, mergedChunk);
  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "desrializing " << size << " pairs from chunks took " << elapsed_s.count() << "s";

  start = chrono::system_clock::now();
  order(env);
  itOrderedChunk = mergedChunk.begin();
  end = chrono::system_clock::now();
  elapsed_s = end - start;
  LOG(INFO) << "ordering " << size << " pairs from chunks took " << elapsed_s.count() << "s";
}

vector<ReduceKVPair>
MergeSortLoader::fetch(int num) {
  auto start = chrono::system_clock::now();

  auto beginIt = itOrderedChunk;
  auto endIt = num <= (int)curSize ? beginIt + num : mergedChunk.end();

  auto res = vector<ReduceKVPair>(beginIt, endIt);
  itOrderedChunk = endIt;
  curSize -= num;

  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "fetch " << res.size() << " pairs from orderedChunk took " << elapsed_s.count() << "s";

  return res;
}

void
MergeSortLoader::order(JNIEnv* env) {
  stable_sort(mergedChunk.begin(), mergedChunk.end(), shuffle::ReduceComparator(env));
}
