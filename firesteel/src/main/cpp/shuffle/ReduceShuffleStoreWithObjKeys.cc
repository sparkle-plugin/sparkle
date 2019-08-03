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
    throw logic_error("Hashmap Loader is not implemented...");
  }
  if (needOrdering) {
    throw logic_error("Mergesort Loader is not implemented...");
  }

  kvPairLoader = new PassThroughLoader(reducerId, idxChunkPtrs);
}

vector<KVPair>
ReduceShuffleStoreWithObjKeys::fetch(JNIEnv* env, int num) {
  vector<KVPair> pairs {kvPairLoader->fetch(num)};

  deserializeKeys(env, pairs);

  return pairs;
}

void
ReduceShuffleStoreWithObjKeys::deserializeKeys(JNIEnv* env, vector<KVPair>& pairs) {
  jclass deserClazz
    {env->FindClass("org/apache/commons/lang3/SerializationUtils")};
  jmethodID deserMid
    {env->GetStaticMethodID(deserClazz, "deserialize", "([B)Ljava/lang/Object;")};
  for (auto& pair : pairs) {

    jbyteArray keyJbyteArray = env->NewByteArray(pair.getSerKeySize());
    env->SetByteArrayRegion(keyJbyteArray, 0, pair.getSerKeySize(), (jbyte*) pair.getSerKey());

    // TODO: delete this ref somewhere.
    jobject key =
      (jobject) env->NewGlobalRef(env->CallStaticObjectMethod(deserClazz, deserMid, keyJbyteArray));
    pair.setKey(key);
  }
}
