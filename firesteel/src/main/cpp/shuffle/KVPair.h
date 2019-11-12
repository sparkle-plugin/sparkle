#ifndef __KVPAIR_H__
#define __KVPAIR_H__

#include <jni.h>
#include <memory>
#include <cstddef>

using namespace std;

/*
 * It it better to decouple the map-side pair and that of reduce-size.
 * The reasons are shown below.
 * 1) Memory management is quite differecne between maps and reduces.
 *    For examaple, serialized value is managed by JVM on the map-side,
 *    but on the other side it is nothing to do with JVM.
 * 2) Eliminate unnecessary setters from the reduce-side pairs.
 */
class MapKVPair {
 public:
 MapKVPair(const jobject& key, int khash, shared_ptr<byte[]> value, int vSize, int partition) :
  key(key), khash(khash), value(value), vSize(vSize), partition(partition) {}
  ~MapKVPair() {}

  inline int getPartition() const {return partition;}
  inline jobject getKey() const {return key;}
  inline void setKey(jobject& _key) {key = _key;};
  inline byte* getSerKey() const {return serKey;}
  inline void setSerKey(byte* bytes) {
    serKey = bytes;
  }
  inline int getSerKeySize() const {return serKeySize;}
  inline void setSerKeySize(int size) {
    serKeySize = size;
  }
  inline int getKeyHash() const {return khash;}
  inline byte* getSerValue() const {return value.get();}
  inline int getSerValueSize() const {return vSize;}

 private:
  jobject key {nullptr};
  int khash {-1};
  byte* serKey {nullptr};
  int serKeySize {-1};

  shared_ptr<byte[]> value {nullptr};
  int vSize {-1};

  int partition {-1};
};

class ReduceKVPair {
public:
 ReduceKVPair(shared_ptr<byte[]> serKey,int serKeySize, int keyHash, shared_ptr<byte[]> value, int vSize, int partition):
    serKey(serKey),
    serKeySize(serKeySize),
    khash(keyHash),
    value(value),
    vSize(vSize), partition(partition) {}
  ~ReduceKVPair() {}

  inline int getPartition() const {return partition;}
  inline jobject getKey() const {return key;}
  inline void setKey(jobject& _key) {key = _key;};
  inline byte* getSerKey() const {return serKey.get();}
  inline int getSerKeySize() const {return serKeySize;}
  inline int getKeyHash() const {return khash;}
  inline byte* getSerValue() const {return value.get();}
  inline int getSerValueSize() const {return vSize;}
private:
  jobject key {nullptr};
  shared_ptr<byte[]> serKey {nullptr};
  int serKeySize {-1};
  int khash {-1};

  shared_ptr<byte[]> value {nullptr};
  int vSize {-1};

  int partition {-1};
};
#endif
