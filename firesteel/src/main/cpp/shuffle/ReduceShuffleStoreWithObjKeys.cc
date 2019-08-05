#include <vector>
#include "ReduceShuffleStoreWithObjKeys.h"

using namespace std;

ReduceShuffleStoreWithObjKeys::
ReduceShuffleStoreWithObjKeys(const ReduceStatus& status,
                              int _reducerId, unsigned char* _buffer,
                              size_t _bufferCapacity, bool ordering,
                              bool aggregation)
  :reduceStatus(status), reducerId(_reducerId),
   buffer(make_pair((byte*)_buffer, _bufferCapacity)),
   needOrdering(ordering), needAggregation(aggregation) {
  if (needAggregation && needOrdering) {
    throw domain_error("this type of reducer is out of scope.");
  }

  vector<pair<region_id, offset>> idxChunkPtrs {toChunkPtrs()};

  if (needAggregation) {
    kvPairLoader = new HashMapLoader(reducerId, idxChunkPtrs);
  } else if (needOrdering) {
    kvPairLoader = new MergeSortLoader(reducerId, idxChunkPtrs);
  } else{
    kvPairLoader = new PassThroughLoader(reducerId, idxChunkPtrs);
  }
}
