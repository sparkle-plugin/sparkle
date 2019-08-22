#ifndef __JNI_UTILS_H__
#define __JNI_UTILS_H__

#include <jni.h>
#include "../shuffle/KVPair.h"

namespace shuffle {
  inline int partialOrdering(int h1, int h2) {
    if (h1 < h2) {
      return -1;
    } else if (h1 == h2) {
      return 0;
    } else {
      return 1;
    }
  }

  class MapComparator {
  public:
  MapComparator(JNIEnv* env) : env(env) {};

    bool operator ()(const MapKVPair& lpair, const MapKVPair& rpair) {
      if (lpair.getPartition() != rpair.getPartition()) {
        return lpair.getPartition() < rpair.getPartition();
      }

      jobject lkey {lpair.getKey()};
      jobject rkey {rpair.getKey()};

      jclass clazz {env->GetObjectClass(lkey)};
      jmethodID compareTo {env->GetMethodID(clazz, "compareTo", "(Ljava/lang/Object;)I")};
      env->ExceptionClear();
      if (compareTo != NULL) {
        int result {env->CallIntMethod(lkey, compareTo, rkey)};
        return result<0;
      }

      // We adopt partial ordering to unorderable keys as a fallback.
      // If you would like to understand partial ordering, please refere to
      // org.apache.spark.util.collection.ExternalSorter.
      jmethodID hashCode {env->GetMethodID(clazz, "hashCode", "()I")};
      int h1 = lkey == nullptr ? 0 : env->CallIntMethod(lkey, hashCode);
      int h2 = rkey == nullptr ? 0 : env->CallIntMethod(rkey, hashCode);
      return partialOrdering(h1, h2);
    }
  private:
    JNIEnv* env = nullptr;
  };

  class ReduceComparator {
  public:
    ReduceComparator(JNIEnv* env) : env(env) {};

    bool operator ()(const ReduceKVPair& lpair, const ReduceKVPair& rpair) {
      jobject lkey {lpair.getKey()};
      jobject rkey {rpair.getKey()};

      jclass clazz {env->GetObjectClass(lkey)};
      jmethodID compareTo {env->GetMethodID(clazz, "compareTo", "(Ljava/lang/Object;)I")};
      env->ExceptionClear();
      if (compareTo != NULL) {
        int result {env->CallIntMethod(lkey, compareTo, rkey)};
        return result<0;
      }

      // We adopt partial ordering to unorderable keys as a fallback.
      // If you would like to understand partial ordering, please refere to
      // org.apache.spark.util.collection.ExternalSorter.
      jmethodID hashCode {env->GetMethodID(clazz, "hashCode", "()I")};
      int h1 = lkey == nullptr ? 0 : env->CallIntMethod(lkey, hashCode);
      int h2 = rkey == nullptr ? 0 : env->CallIntMethod(rkey, hashCode);
      return partialOrdering(h1, h2);
    }
  private:
    JNIEnv* env = nullptr;
  };
}
#endif
