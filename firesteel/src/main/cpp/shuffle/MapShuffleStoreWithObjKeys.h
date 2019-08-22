#ifndef MAPSHUFFLESTORE_WITH_OBJ_KEYS_
#define MAPSHUFFLESTORE_WITH_OBJ_KEYS_

#include <jni.h>
#include <memory>
#include <glog/logging.h>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <cstddef>
#include <utility>

#include "GenericMapShuffleStore.h"
#include "EnumKvTypes.h"
#include "KVPair.h"
#include "MapStatus.h"

using namespace std;

class MapShuffleStoreWithObjKeys: public GenericMapShuffleStore {
public:
  MapShuffleStoreWithObjKeys(int mapId, bool ordering);
  ~MapShuffleStoreWithObjKeys() {};

  void storeKVPairs (vector<jobject>& keys, unsigned char *values,
    int* voffsets, int* partitions, int numPairs);

  /**
   * Write stored Key-Value pairs into the shared memory.
   * If need ordering, sort the pairs before write.
   */
  unique_ptr<MapStatus> write(JNIEnv* env);
  void deleteJobjectKeys(JNIEnv* env);

  KValueTypeDefinition getKValueType() override {
    return kValueTypeDef;
  }

  VValueTypeDefinition getVValueType() override {
    return vValueTypeDef;
  }

  bool needsOrdering() override {
    return doOrdering;
  }

  void stop () override {
    // Nothing to do.
    // the stop func is for cleaning up local bufferes.
  }

  void shutdown() override;

private:
  struct NativeMapStatus {
    pair<uint64_t, uint64_t> indexChunkAddr;
    vector<int> bucketSizes;
  };

  const KValueTypeDefinition kValueTypeDef {KValueTypeId::Object};
  VValueTypeDefinition vValueTypeDef {};
  const int mapId;
  const bool doOrdering;
  vector<MapKVPair> kvPairs;

  int numPartitions {0};

  typedef uint64_t regionid_t;
  typedef uint64_t offset_t;
  pair<regionid_t, offset_t> idxChunkPtr = make_pair(0,0);
  vector<pair<regionid_t, offset_t>> dataChunkPtrs;

  void sortPairs(JNIEnv* env);
  void serializeKeys(JNIEnv* env);
  /**
   * dataChunkLocalOffsets: local pointer of Data Chunks.
   * mapStatus: Stats of this map-id.
   */
  void writeIndexChunk(vector<byte*>& dataChunkLocalOffsets, NativeMapStatus& mapStatus);
  void writeDataChunk(vector<byte*>& localOffsets);
};

#endif
