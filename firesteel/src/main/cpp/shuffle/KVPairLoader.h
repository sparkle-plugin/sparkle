#ifndef __KVPAIRLOADER_H_
#define __KVPAIRLOADER_H_

#include <vector>
#include <utility>
#include <cstdint>
#include "KVPair.h"

using namespace std;

typedef uint64_t region_id;
typedef uint64_t offset;
typedef uint64_t chunk_id;
typedef vector<KVPair> chunk;

class KVPairLoader {
public:
  KVPairLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : reducerId(_reducerId), chunkPtrs(_chunkPtrs) {
    size = load(reducerId);
  }
  virtual ~KVPairLoader() {}
  /**
   * load the whole chunks in memory as kv pairs.
   */
  size_t load(int reducerId);
  /**
   * fetch the number of kv pairs.
   * Note: we should call `load` before this method.
   */
  virtual vector<KVPair> fetch(int num)=0;
protected:
  const int reducerId;
  vector<pair<region_id, offset>>& chunkPtrs; //index chunk pointers.
  vector<pair<chunk_id, chunk>> dataChunks;
  uint64_t size {0};
  byte* dropUntil(int partitionId, byte* indexChunkPtr);
};

class PassThroughLoader : public KVPairLoader {
public:
  PassThroughLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : KVPairLoader(_reducerId, _chunkPtrs) {
    flatten();
  }
  ~PassThroughLoader() {
    for (KVPair& pair : flatChunk) {
      delete [] pair.getSerKey();
      delete [] pair.getSerValue();
    }
  }

  vector<KVPair> fetch(int num) override;

private:
  vector<KVPair> flatChunk;
  void flatten();
};
#endif
