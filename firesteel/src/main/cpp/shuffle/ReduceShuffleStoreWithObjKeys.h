#ifndef REDUCE_SHUFFLE_STORE_WITH_OBJ_KEYS_H__
#define REDUCE_SHUFFLE_STORE_WITH_OBJ_KEYS_H__

#include <vector>
#include <stdexcept>
#include <utility>
#include "MapStatus.h"
#include "GenericReduceShuffleStore.h"

using namespace std;

class ReduceShuffleStoreWithObjKeys: public GenericReduceShuffleStore {
 public:
  ReduceShuffleStoreWithObjKeys(const ReduceStatus& status,
                                int _reducerId, unsigned char* _buffer,
                                size_t _bufferCapacity, bool ordering,
                                bool aggregation)
    :reduceStatus(status), reducerId(_reducerId),
     buffer(make_pair((byte*)_buffer, _bufferCapacity)),
     needOrdering(ordering), needAggregation(aggregation) {}
  ~ReduceShuffleStoreWithObjKeys() {}

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

  void stop() override {}
  void shutdown() override {}

 private:
  const ReduceStatus& reduceStatus;
  const int reducerId;
  const pair<byte*, size_t> buffer;
  const bool needOrdering;
  const bool needAggregation;
  const KValueTypeDefinition kvTypeDefinition {KValueTypeId::Object};
  VValueTypeDefinition vvTypeDefinition;
};
#endif
