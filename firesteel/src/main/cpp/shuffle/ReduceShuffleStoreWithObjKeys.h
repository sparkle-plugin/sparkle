#ifndef REDUCE_SHUFFLE_STORE_WITH_OBJ_KEYS_H__
#define REDUCE_SHUFFLE_STORE_WITH_OBJ_KEYS_H__

#include <jni.h>
#include <vector>
#include <stdexcept>
#include <utility>
#include "MapStatus.h"
#include "GenericReduceShuffleStore.h"
#include "KVPairLoader.h"
#include "EnumKvTypes.h"

using namespace std;

class ReduceShuffleStoreWithObjKeys: public GenericReduceShuffleStore {
 public:
  ReduceShuffleStoreWithObjKeys(const ReduceStatus& status,
                                int _reducerId, unsigned char* _buffer,
                                size_t _bufferCapacity, bool ordering,
                                bool aggregation);
  ~ReduceShuffleStoreWithObjKeys() {
    delete kvPairLoader;
  }

  inline KValueTypeDefinition getKValueType() override {
    return kvTypeDefinition;
  }

  VValueTypeDefinition getVValueType() override {
    // Who needs this method..
    throw logic_error("getVValueType is not implemented.");
    return vvTypeDefinition;
  }

  inline bool needsOrdering() override {
    return needOrdering;
  }

  inline bool needsAggregation() override {
    return needAggregation;
  }

  inline void prepare(JNIEnv* env) {
    kvPairLoader->prepare(env);
  }

  inline vector<KVPair> fetch(int num) {
    return kvPairLoader->fetch(num);
  }

  inline vector<vector<KVPair>> fetchAggregatedPairs(int num) {
    return kvPairLoader->fetchAggregatedPairs(num);
  }

  void stop() override {}
  void shutdown() override {}

 private:
  const ReduceStatus& reduceStatus;
  const int reducerId;
  const pair<byte*, size_t> buffer;
  const bool needOrdering;
  const bool needAggregation;
  const KValueTypeDefinition kvTypeDefinition
    {KValueTypeDefinition(static_cast<KValueTypeId>(6))};
  VValueTypeDefinition vvTypeDefinition;

  KVPairLoader* kvPairLoader {nullptr};

  inline vector<pair<region_id, offset>> toChunkPtrs() {
    vector<pair<region_id, offset>> pairs;
    for (auto bucket : reduceStatus.mapBuckets) {
      pairs.push_back(make_pair(bucket.regionId, bucket.offset));
    }
    return pairs;
  }
};
#endif
