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
#include "../jnishuffle/JniUtils.h"

using namespace std;
using namespace alps;

size_t
KVPairLoader::load(int reducerId) {
  // decode the index chunk.
  vector<pair<region_id, offset>> dataChunkPtrs;
  vector<int> numPairs;
  for (auto& chunkPtr : chunkPtrs) {
    RRegion::TPtr<void> idxChunkPtr(chunkPtr.first, chunkPtr.second);
    byte* index = (byte*)idxChunkPtr.get();
    {
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

      dataChunkPtrs.emplace_back(rid, roffset);
      numPairs.emplace_back(numPair);
    }
  }

  // decode data chunks.
  size_t numKVPairs {0};
  for (size_t i=0; i<dataChunkPtrs.size(); ++i) {
    RRegion::TPtr<void> dataChunkPtr(dataChunkPtrs[i].first, dataChunkPtrs[i].second);
    byte* index = (byte*) dataChunkPtr.get();

    dataChunks.emplace_back(i, chunk());
    dataChunks[i].second.reserve(numPairs[i]);
    for (int j=0; j<numPairs[i]; ++j) {
      // [serKeySize, serKey, serValueSize, serValue]
      {
        int serKeySize;
        memcpy(&serKeySize, index, sizeof(int));
        index += sizeof(int);

        shared_ptr<byte[]> serKey(new byte[serKeySize]);
        memcpy(serKey.get(), index, serKeySize);
        index += serKeySize;

        int serValueSize;
        memcpy(&serValueSize, index, sizeof(int));
        index += sizeof(int);

        shared_ptr<byte[]> serValue(new byte[serValueSize]);
        memcpy(serValue.get(), index, serValueSize);
        index += serValueSize;

        dataChunks[i].second.emplace_back(serKey, serKeySize, serValue, serValueSize, reducerId);
      }
    }
    numKVPairs += dataChunks[i].second.size();
  }

  return numKVPairs;
}

byte*
KVPairLoader::dropUntil(int partitionId, byte* index) {
  for (int i=0; i<partitionId; ++i) {
    index += sizeof(region_id) + sizeof(offset) + sizeof(int)*2;
  }

  return index;
}

void
PassThroughLoader::prepare(JNIEnv* env) {
  // flatten data chunks.
  auto start = chrono::system_clock::now();
  flatten();
  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "flatten " << size << " pairs took " << elapsed_s.count() << "s";
}

vector<ReduceKVPair>
PassThroughLoader::fetch(int num) {
  auto start = chrono::system_clock::now();

  auto beginIt = itFlatChunk;
  auto endIt = beginIt + min(num, static_cast<int>(flatChunk.size()));
  auto res = vector<ReduceKVPair>(beginIt, endIt);
  itFlatChunk = endIt;

  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "fetch " << res.size() << " pairs from flatChunk took " << elapsed_s.count() << "s";

  return res;
}

void
PassThroughLoader::flatten() {
  for (auto&& [chunk_id, chunk] : dataChunks) {
    flatChunk.insert(flatChunk.end(),
                     make_move_iterator(chunk.begin()),
                     make_move_iterator(chunk.end()));
  }
}

void
HashMapLoader::prepare(JNIEnv* env) {
  auto start = chrono::system_clock::now();
  for (auto&& [idx, dataChunk] : dataChunks) {
    shuffle::deserializeKeys(env, dataChunk);
  }
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
    make_unique<unordered_map<jobject, vector<ReduceKVPair>, Hasher, EqualTo>>(0, Hasher(env), EqualTo(env));
  for (auto&& [idx, dataChunk] : dataChunks) {
    for (auto& pair : dataChunk) {
      auto it {hashmap->find(pair.getKey())};
      if (it == hashmap->end()) {
	 hashmap->emplace(pair.getKey(), vector<ReduceKVPair>{pair});
        continue;
      }

      auto& pairs {it->second};
      pairs.emplace_back(move(pair));
    }
  }

  hashmapIt = hashmap->begin();
}

vector<vector<ReduceKVPair>>
HashMapLoader::fetchAggregatedPairs(int num) {
  auto start = chrono::system_clock::now();

  vector<vector<ReduceKVPair>> res;
  auto beginIt = hashmapIt;
  auto endIt = next(beginIt, min(num, static_cast<int>(hashmap->size())));

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
  for (auto&& [idx, dataChunk] : dataChunks) {
    shuffle::deserializeKeys(env, dataChunk);
  }
  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "desrializing " << size << " pairs from chunks took " << elapsed_s.count() << "s";

  start = chrono::system_clock::now();
  order(env);
  end = chrono::system_clock::now();
  elapsed_s = end - start;
  LOG(INFO) << "ordering " << size << " pairs from chunks took " << elapsed_s.count() << "s";
}

vector<ReduceKVPair>
MergeSortLoader::fetch(int num) {
  auto start = chrono::system_clock::now();

  auto beginIt = itOrderedChunk;
  auto endIt = beginIt + min(num, static_cast<int>(orderedChunk.size()));
  auto res = vector<ReduceKVPair>(beginIt, endIt);
  itOrderedChunk = endIt;

  auto end = chrono::system_clock::now();
  chrono::duration<double> elapsed_s = end - start;
  LOG(INFO) << "fetch " << res.size() << " pairs from orderedChunk took " << elapsed_s.count() << "s";

  return res;
}

void
MergeSortLoader::order(JNIEnv* env) {
  for (auto&& [chunk_id, chunk] : dataChunks) {
    orderedChunk.insert(orderedChunk.end(),
			make_move_iterator(chunk.begin()),
			make_move_iterator(chunk.end()));
  }

  stable_sort(orderedChunk.begin(), orderedChunk.end(), shuffle::ReduceComparator(env));
}
