#ifndef __JNI_UTILS_H__
#define __JNI_UTILS_H__

#include <jni.h>
#include "../shuffle/KVPair.h"

namespace shuffle {
  class Comparator {
  public:
  Comparator(JNIEnv* env) : env(env) {};

    bool operator ()(const KVPair& lpair, const KVPair& rpair) {
      if (lpair.getPartition() != rpair.getPartition()) {
        return lpair.getPartition() < rpair.getPartition();
      }

      jobject lkey {lpair.getKey()};
      jobject rkey {rpair.getKey()};

      jclass clazz {env->GetObjectClass(lkey)};
      jmethodID compareTo {env->GetMethodID(clazz, "compareTo", "(Ljava/lang/Object;)I")};
      if (compareTo != NULL) {
        int result {env->CallIntMethod(lkey, compareTo, rkey)};
        return result<0;
      }

      // We adopt partial ordering to comparable keys as a fallback.
      // If you would like to understand partial ordering, please refere to
      // org.apache.spark.util.collection.ExternalSorter.
      env->ExceptionClear();
      jmethodID hashCode {env->GetMethodID(clazz, "hashCode", "()I")};
      int h1 =
        lkey == nullptr ? 0 : env->CallIntMethod(lkey, hashCode);
      int h2 =
        rkey == nullptr ? 0 : env->CallIntMethod(rkey, hashCode);
      if (h1 < h2) {
        return -1;
      } else if (h1 == h2) {
        return 0;
      } else {
        return 1;
      }
    }
  private:
    JNIEnv* env = nullptr;
  };
}
#endif
