#ifndef __REDUCE_SHUFFLE_STORE_LIKE__
#define __REDUCE_SHUFFLE_STORE_LIKE__

#include <stdexcept>
#include "GenericReduceShuffleStore.h"
#include "EnumKvTypes.h"
#include "MapStatus.h"

using namespace std;

/*
 * This class does not reduce loaded shuffle data.
 * Just copy target partitions into DirectBuffer to reduce it in Java.
 */
class ReduceShuffleStoreLike: public GenericReduceShuffleStore {
 public:
  ReduceShuffleStoreLike() {}
  ~ReduceShuffleStoreLike() {}

  inline KValueTypeDefinition getKValueType() override {
    return kvTypeDefinition;
  }

  VValueTypeDefinition getVValueType() override {
    // Who needs this method..
    throw logic_error("getVValueType is not implemented.");
    return vvTypeDefinition;
  }

  inline bool needsOrdering() override {
    return false;
  }
  inline bool needsAggregation() override {
    return false;
  }

  void stop() override {
    // NOOP.
  }
  void shutdown() override{
    // NOOP.
  }

  // Need num of pairs to stop loading..
  void load(byte* buffer, int reducerId, ReduceStatus& status);

  // ShuffleReade Stats
  inline long getNumDataChunksRead() {
    return reduceStats.numDataChunksRead;
  }
  inline long getBytesDataChunksRead() {
    return reduceStats.bytesDataChunksRead;
  }
  inline long getNumRemoteDataChunksRead() {
    return reduceStats.numRemoteDataChunksRead;
  }
  inline long getBytesRemoteDataChunksRead() {
    return reduceStats.bytesRemoteDataChunksRead;
  }
  inline long getNumKVPairsRead() {
    return reduceStats.numKVPairsRead;
  }

 private:
  struct Stats {
    uint64_t numDataChunksRead {0};
    uint64_t bytesDataChunksRead {0};
    uint64_t numKVPairsRead {0};
    uint64_t numRemoteDataChunksRead {0};
    uint64_t bytesRemoteDataChunksRead {0};
  } reduceStats;

  const KValueTypeDefinition kvTypeDefinition
    {KValueTypeDefinition(static_cast<KValueTypeId>(6))};
  VValueTypeDefinition vvTypeDefinition;

};

#endif
