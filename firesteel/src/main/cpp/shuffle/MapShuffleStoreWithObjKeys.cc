#include <jni.h>
#include <vector>
#include <algorithm>
#include <cstddef>
#include <utility>
#include "MapShuffleStoreWithObjKeys.h"
#include "KVPair.h"
#include "ShuffleConstants.h"
#include "ShuffleStoreManager.h"
#include "ShuffleDataSharedMemoryManager.h"
#include "ShuffleDataSharedMemoryManagerHelper.h"
#include "MapStatus.h"

using namespace std;

MapShuffleStoreWithObjKeys::
MapShuffleStoreWithObjKeys(int mapId, bool ordering)
  : mapId(mapId), doOrdering(ordering) {}

void
MapShuffleStoreWithObjKeys::storeKVPairs(
    vector<jobject>& keys, unsigned char *values,
    int* voffsets, int* partitions, int numPairs) {

  for (int i=0; i<numPairs; ++i) {
    KVPair pair = KVPair(keys[i] , values, *(voffsets+i), *(partitions+i));
    kvPairs.push_back(pair);

    // update # of partitions.
    numPartitions = max(numPartitions, *(partitions+i));
  }

  return ;
}

void
MapShuffleStoreWithObjKeys::write(JNIEnv* env, MapStatus* mapStatus) {
  if (kvPairs.empty()) {
    return ;
  }

  if (needsOrdering()) {
    sortPairs(env);
  }
  serializeKeys(env);

  // transfer kvPairs to the global0.
  vector<byte*> offsets;
  NativeMapStatus stats;
  writeIndexChunk(offsets, stats);
  writeDataChunk(offsets);

  // fill MapStatus with corresponding stats.
  mapStatus = new MapStatus(stats.indexChunkAddr.first, stats.indexChunkAddr.second,
                            numPartitions, mapId);
  for (int i=0; i<(int)stats.bucketSizes.size(); ++i) {
    mapStatus->setBucketSize(i, stats.bucketSizes[i]);
  }
}

void
MapShuffleStoreWithObjKeys::deleteJobjectKeys(JNIEnv* env) {
  for (auto pair : kvPairs) {
    env->DeleteGlobalRef(pair.getKey());
  }
}

void
MapShuffleStoreWithObjKeys::shutdown() {
  LOG(INFO) << "map shuffle store with obj keys with mapId: " << mapId << " is shutting down";

  ShuffleDataSharedMemoryManager *memoryManager
    = ShuffleStoreManager::getInstance()->getShuffleDataSharedMemoryManager();

  {
    RRegion::TPtr<void> gptr(idxChunkPtr.first, idxChunkPtr.second);
    memoryManager->free_indexchunk(gptr);
  }

  for (auto ptr : dataChunkPtrs) {
    RRegion::TPtr<void> gptr(ptr.first, ptr.second);
    memoryManager->free_datachunk(gptr);
  }
}


/*
 * private methods
 */

void
MapShuffleStoreWithObjKeys::sortPairs(JNIEnv* env) {
  class Comparator {
  public:
    Comparator(JNIEnv* env) : jenv(env) {};

    bool operator ()(const KVPair& lpair, const KVPair& rpair) {
      if (lpair.getPartition() != rpair.getPartition()) {
        return lpair.getPartition() < rpair.getPartition();
      }

      jobject lkey {lpair.getKey()};
      jobject rkey {rpair.getKey()};

      jclass clazz {jenv->GetObjectClass(lkey)};
      jmethodID compareTo {jenv->GetMethodID(clazz, "compareTo", "(Ljava/lang/Object;)I")};
      int result {jenv->CallIntMethod(lkey, compareTo, rkey)};

      return result<0;
    }

  private:
    JNIEnv* jenv = nullptr;
  };

  stable_sort(kvPairs.begin(), kvPairs.end(), Comparator(env));
}

void
MapShuffleStoreWithObjKeys::serializeKeys(JNIEnv* env) {
  jclass serClazz
    {env->FindClass("org/apache/commons/lang3/SerializationUtils")};
  jmethodID serMid
    {env->GetStaticMethodID(serClazz, "serialize", "(Ljava/io/Serializable;)[B")};
  for (auto kvPair : kvPairs) {
    jbyteArray byteArray
      = (jbyteArray) env->CallStaticObjectMethod(serClazz, serMid, kvPair.getKey());

    // TODO: Check this might be a local ref.
    jbyte* bytes = env->GetByteArrayElements(byteArray , NULL);
    kvPair.setSerKey(reinterpret_cast<byte*>(bytes));
    kvPair.setSerKeySize(env->GetArrayLength(byteArray));
  }
}

void
MapShuffleStoreWithObjKeys::writeIndexChunk(vector<byte*>& dataChunkLocalOffsets, NativeMapStatus& mapStatus) {
  static_assert(SHMShuffleGlobalConstants::USING_RMB, "RMB should be enabled.");

  ShuffleDataSharedMemoryManager *memoryManager {
    ShuffleStoreManager::getInstance()->getShuffleDataSharedMemoryManager()};
  RRegion::TPtr<void> global_null_ptr;
  size_t indexChunkSize =
    sizeof(int) // Key's type.
    + sizeof(int) // # of buckets(=partitions)
    // # of (regionId, offset, sizeof(bucket), sizeof(numPairs)) for each partition.
    + numPartitions * (sizeof(uint64_t)*2 + sizeof(int)*2);

  RRegion::TPtr<void> indexChunkGlobalPointer
    = memoryManager->allocate_indexchunk (indexChunkSize);
  assert(indexChunkGlobalPointer != global_null_ptr);
  mapStatus.indexChunkAddr
    = make_pair(indexChunkGlobalPointer.region_id(), indexChunkGlobalPointer.offset());
  idxChunkPtr = mapStatus.indexChunkAddr;
  byte* localOffset = (byte*) indexChunkGlobalPointer.get();

  {
    int keyTypeId {KValueTypeId::Object};
    memcpy(localOffset, &keyTypeId, sizeof(KValueTypeId));
    localOffset += sizeof(KValueTypeId);
  }

  {
    memcpy(localOffset, &numPartitions, sizeof(int));
    localOffset += sizeof(int);
  }

  // alloc data chunks, then write their meta data into the index chunk.
  vector<int> bucketSizes(numPartitions); // byte
  fill(bucketSizes.begin(), bucketSizes.end(), 0);
  // # of pairs(not aggregated) for each partition.
  vector<int> numPairs(numPartitions);
  fill(numPairs.begin(), numPairs.end(), 0);

  for (auto pair : kvPairs) {
    bucketSizes[pair.getPartition()] += pair.getSerKeySize();
    numPairs[pair.getPartition()] += 1;
  }

  for (int i=0; i<numPartitions; ++i) {
    // keep BucketSize before hand.
    mapStatus.bucketSizes.push_back(bucketSizes[i]);

    // Allocate Data Chuncks using bucketSize.
    // Then, keep the (regionId, offset) pairs in this instance.
    RRegion::TPtr<void> chunk
      = memoryManager->allocate_datachunk(bucketSizes[i]);
    assert(chunk != global_null_ptr);
    dataChunkPtrs.push_back(make_pair(chunk.region_id(), chunk.offset()));

    {
      uint64_t regionId {chunk.region_id()};
      memcpy(localOffset, &regionId, sizeof(uint64_t));
      localOffset += sizeof(uint64_t);

      uint64_t chunkOffset {chunk.offset()};
      memcpy(localOffset, &chunkOffset, sizeof(uint64_t));
      localOffset += sizeof(uint64_t);

      memcpy(localOffset, &bucketSizes[i], sizeof(int));
      localOffset += sizeof(int);

      memcpy(localOffset, &numPairs[i], sizeof(int));
      localOffset += sizeof(int);
    }

    // save the data chunk head pointers to store pairs into this chunk.
    dataChunkLocalOffsets.push_back((byte*) chunk.get());
  }
}

void
MapShuffleStoreWithObjKeys::writeDataChunk(vector<byte*>& localOffsets) {
  /*
   * [(key-size, serKey, value-size, serValue)] for each partition.
   */
  for (auto pair : kvPairs) {
    byte* localOffset = localOffsets[pair.getPartition()];

    {
      int keySize = pair.getSerKeySize();
      memcpy(localOffset, &keySize, sizeof(int));
      localOffset += sizeof(int);

      memcpy(localOffset, pair.getSerKey(), keySize);
      localOffset += keySize;
    }

    {
      int valueSize = pair.getSerValueSize();
      memcpy(localOffset, &valueSize, sizeof(int));
      localOffset += sizeof(int);

      memcpy(localOffset, pair.getSerValue(), valueSize);
      localOffset += valueSize;
    }
  }
}
