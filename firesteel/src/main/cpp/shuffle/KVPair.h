#ifndef __KVPAIR_H__
#define __KVPAIR_H__

#include<cstddef>
#include<jni.h>

using namespace std;

class KVPair {
 public:
  KVPair(const jobject& key, unsigned char* value, int vSize, int partition) :
    key(key), value(value), vSize(vSize), partition(partition) {}
  ~KVPair() {}

  inline int getPartition() const {return partition;}
  inline jobject getKey() const {return key;}
  inline byte* getSerKey() const {return serKey;}
  inline void setSerKey(byte* bytes) {
    serKey = bytes;
  }
  inline int getSerKeySize() const {return serKeySize;}
  inline void setSerKeySize(int size) {
    serKeySize = size;
  }
  inline byte* getSerValue() const {return (byte*) value;}
  inline int getSerValueSize() const {return vSize;}

 private:
  /*
   * TODO:
   * considering memory footprint it's better use variants for (ser)key.
   * Please refer to https://en.cppreference.com/w/cpp/utility/variant
   */
  jobject key;
  byte* serKey {nullptr};
  int serKeySize {-1};
  unsigned char* value;
  int vSize;
  int partition;
};
#endif
