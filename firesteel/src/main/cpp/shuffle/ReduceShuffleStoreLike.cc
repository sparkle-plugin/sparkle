#include <cstring>
#include "globalheap/globalheap.hh"
#include "ReduceShuffleStoreLike.h"
#include "SimpleUtils.h"

using namespace alps;
using namespace std;

void
ReduceShuffleStoreLike::load(byte* buffer, int reducerId, ReduceStatus& status) {
  // FIXME: for suuport lazy fetch we should know the number of pairs in each bucket.
  for (auto bucket : status.mapBuckets) {
    // index chunk -> data chunk -> byte*
    RRegion::TPtr<void> idxChunkPtr(bucket.regionId, bucket.offset);
    byte *offset = (byte*) idxChunkPtr.get();

    int bucketNodeId {-1};
    memcpy(&bucketNodeId, offset, sizeof(int));
    offset += sizeof(int);

    int numPartitions {-1};
    memcpy(&numPartitions, offset, sizeof(int));
    offset += sizeof(int);

    // skip other partitions.
    offset += (sizeof(uint64_t)*2 + sizeof(int)) * reducerId;

    uint64_t regionId {0};
    uint64_t doffset {0};
    int bucketSize {-1};
    memcpy(&regionId, offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    memcpy(&doffset, offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    memcpy(&bucketSize, offset, sizeof(int));
    offset += sizeof(int);

    if (bucketSize <= 0) {
      continue;
    }

    RRegion::TPtr<void> dataChunkPtr(regionId, doffset);
    memcpy(buffer, dataChunkPtr.get(), bucketSize);
    buffer += bucketSize;

    // update stats.
    int curNodeId {OsUtil::getCurrentNumaNode()};
    if (bucketNodeId == curNodeId) {
      reduceStats.numDataChunksRead++;
      reduceStats.bytesDataChunksRead += bucketSize;
    } else {
      reduceStats.numRemoteDataChunksRead++;
      reduceStats.bytesRemoteDataChunksRead += bucketSize;
    }
  }
}
