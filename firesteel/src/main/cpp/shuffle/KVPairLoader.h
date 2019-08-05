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
  virtual ~KVPairLoader() {}
  /**
   * load the whole chunks in memory as kv pairs.
   */
  virtual size_t load(int reducerId) =0;
  /**
   * fetch the number of kv pairs.
   * Note: we should call `load` before this method.
   */
  virtual vector<KVPair> fetch(int num)=0;
protected:
  byte* dropUntil(int partitionId, byte* indexChunkPtr);
};

class PassThroughLoader : public KVPairLoader {
public:
  PassThroughLoader(int _reducerId, vector<pair<region_id, offset>>& _chunkPtrs)
    : reducerId(_reducerId), chunkPtrs(_chunkPtrs) {
    size = load(reducerId);
    flatten();
  }
  ~PassThroughLoader() {
    for (KVPair& pair : flatChunk) {
      delete [] pair.getSerKey();
      delete [] pair.getSerValue();
    }
  }

  size_t load(int reducerId) override; // Ugly: this should be in super class....
  vector<KVPair> fetch(int num) override;

private:
  const int reducerId;
  vector<pair<region_id, offset>>& chunkPtrs; //index chunk pointers.
  vector<pair<chunk_id, chunk>> dataChunks;
  vector<KVPair> flatChunk;
  uint64_t size {0};

  void flatten();
};
#endif
