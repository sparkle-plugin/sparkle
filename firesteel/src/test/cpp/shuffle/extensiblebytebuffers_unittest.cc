/*
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "ExtensibleByteBuffers.h"
#include <string>
#include <vector>
#include <iostream>
#include <random>
#include <functional>

#include "gtest/gtest.h"
#include "glog/logging.h"

using namespace std;


//utility class 1
class Rand_int {
  //store the parameter
public: 
  Rand_int (int lo, int hi) : p {lo, hi} { }
  int operator()() const {
    return r();
  };

private:
  uniform_int_distribution<>::param_type p;
  function<int()> r = bind (uniform_int_distribution<>{p}, default_random_engine{});
};

//utilty funtion 1 
size_t hash_compute (unsigned char* buffer, int payload_size){
  size_t result = 0;
  size_t prime = 31;
  std::hash<unsigned char> hash_fn;
  for (int i=0; i<payload_size;i++) {
    result = result *prime +  hash_fn (buffer[i]);
  }

  return result;
}


TEST(ExtensibleByteBuffersTest, SingleRecordsWithIntKeySmallRecord ) {


  // Seed with a real random value, if available
  Rand_int randomGenerator {0, 120};
  
  int buffer_size=1*1024;
  int payload_size = buffer_size /4; 
  
  unsigned char *buffer = (unsigned char*)malloc (sizeof (unsigned char)*payload_size);
  //populate randome number to it 
  for (int i=0; i<payload_size; i++) {
      buffer[i] = (unsigned char) randomGenerator(); 
      //cout << (int)buffer[i] <<endl;
  }

  ExtensibleByteBuffers  bufferManager (buffer_size);

  PositionInExtensibleByteBuffer posBuffer = bufferManager.append(buffer, payload_size); 

  EXPECT_EQ (posBuffer.start_buffer, 0);
  EXPECT_EQ (posBuffer.position_in_start_buffer, 0);
  EXPECT_EQ (posBuffer.value_size, payload_size);   
  
  //now read it back 
  unsigned char *buffer2 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size);
  bufferManager.retrieve(posBuffer, buffer2);

  //compare two results
  size_t hashResult = hash_compute (buffer, payload_size);
  //cout << "TEST: first hash result: " << hashResult << endl; 
  size_t hashResult2 = hash_compute(buffer2, payload_size);
  //cout << "TEST: second hash result: " << hashResult << endl; 

  EXPECT_EQ  (hashResult, hashResult2);

  free(buffer);
  free(buffer2);

}

TEST(ExtensibleByteBuffersTest, SingleRecordsWithIntKeyBigRecord1 ) {


  // Seed with a real random value, if available
  Rand_int randomGenerator {0, 240};
  
  int buffer_size=1*1024;
  int extra_bytes = 107;
  int payload_size = buffer_size  + extra_bytes; 
  
  unsigned char *buffer = (unsigned char*)malloc (sizeof (unsigned char)*payload_size);
  //populate randome number to it 
  for (int i=0; i<payload_size; i++) {
      buffer[i] = (unsigned char) randomGenerator(); 
  }

  ExtensibleByteBuffers  bufferManager (buffer_size);

  PositionInExtensibleByteBuffer posBuffer = bufferManager.append(buffer, payload_size); 

  EXPECT_EQ (posBuffer.start_buffer, 0);
  EXPECT_EQ (posBuffer.position_in_start_buffer, 0);
  EXPECT_EQ (posBuffer.value_size, payload_size);   

  int current_buffer_position = bufferManager.current_buffer_position();
  EXPECT_EQ (current_buffer_position, 1);

  ByteBufferHolder &currentHolder = bufferManager.current_buffer();
  EXPECT_EQ (currentHolder.remain_capacity(), buffer_size-extra_bytes);
  EXPECT_EQ (currentHolder.position_in_buffer(), extra_bytes);

  //now read it back 
  unsigned char *buffer2 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size);
  bufferManager.retrieve(posBuffer, buffer2);

  //compare two results
  size_t hashResult = hash_compute (buffer, payload_size);
  //cout << "TEST: first hash result: " << hashResult << endl; 
  size_t hashResult2 = hash_compute(buffer2, payload_size);
  //cout << "TEST: second hash result: " << hashResult << endl; 

  EXPECT_EQ  (hashResult, hashResult2);

  free(buffer);
  free(buffer2);

}


TEST(ExtensibleByteBuffersTest, TwoRecordsWithIntKeyWithBigRecord) {
  int buffer_size=1*1024;
  // Seed with a real random value, if available
  Rand_int randomGenerator {0, 240};

  //buffer 1.
  int payload_size1= 298; 
  unsigned char *buffer1 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size1);
  //populate randome number to it 
  for (int i=0; i<payload_size1; i++) {
      buffer1[i] = (unsigned char) randomGenerator(); 
  }

  //buffer 2
  int extra_bytes = 107;
  int payload_size2 = buffer_size  + extra_bytes; 
  unsigned char *buffer2 = (unsigned char*)malloc (sizeof (unsigned char)*payload_size2);
  //populate randome number to it 
  for (int i=0; i<payload_size2; i++) {
      buffer2[i] = (unsigned char) randomGenerator(); 
  }

  ExtensibleByteBuffers  bufferManager (buffer_size);

  PositionInExtensibleByteBuffer posBuffer1 = bufferManager.append(buffer1, payload_size1); 
  PositionInExtensibleByteBuffer posBuffer2 = bufferManager.append(buffer2, payload_size2); 

  //now read it back 
  unsigned char *buffer1c = (unsigned char*)malloc (sizeof (unsigned char)*payload_size1);
  unsigned char *buffer2c = (unsigned char*)malloc (sizeof (unsigned char)*payload_size2);
  bufferManager.retrieve(posBuffer1, buffer1c);
  bufferManager.retrieve(posBuffer2, buffer2c);

  //compare two results
  size_t hashResult1 = hash_compute (buffer1, payload_size1);
  size_t hashResult1c = hash_compute (buffer1c, payload_size1);
  //cout << "TEST: first hash result: " << hashResult << endl; 
  size_t hashResult2 = hash_compute(buffer2, payload_size2);
  size_t hashResult2c = hash_compute(buffer2c, payload_size2);
  //cout << "TEST: second hash result: " << hashResult << endl; 

  EXPECT_EQ  (hashResult1, hashResult1c);
  EXPECT_EQ  (hashResult2, hashResult2c);

  free(buffer1);
  free(buffer1c);
  free(buffer2);
  free(buffer2c);

}


TEST(ExtensibleByteBuffersTest, NRecordsWithIntKeyWithBigRecords) {
  int buffer_size=1*1024;
  // Seed with a real random value, if available
  Rand_int randomGenerator_buffersize {1, 10240}; //needs to be at least 1.
  Rand_int randomGenerator_element {0, 240}; //unsigned char
  
  vector <int> buffersizes; 
  vector <unsigned char *> buffers; 
  vector <unsigned char *> retrieved_buffers;
  vector <PositionInExtensibleByteBuffer> posBuffers; 


  int totalN = 100; 

  for (int i=0; i<totalN; i++) {
    int buffer_size =randomGenerator_buffersize();
    //cout << buffer_size <<endl;
    buffersizes.push_back(buffer_size);
    unsigned char *buffer = (unsigned char*)malloc (sizeof (unsigned char)*buffer_size);
    for (int k=0; k<buffer_size; k++) {
        buffer[k] = (unsigned char) randomGenerator_element(); 
    }    
    buffers.push_back(buffer);
  }

  ExtensibleByteBuffers  bufferManager (buffer_size);

  for (int i=0; i<totalN; i++) {
      unsigned char *buffer = buffers[i];
      int buffer_size = buffersizes[i];
      PositionInExtensibleByteBuffer posBuffer = bufferManager.append(buffer, buffer_size); 
      posBuffers.push_back(posBuffer);
  }

  //now read it back 
  for (int i=0; i<totalN; i++) {
    int buffer_size = buffersizes[i];
    PositionInExtensibleByteBuffer posBuffer = posBuffers[i];
    unsigned char *retrieved_buffer = (unsigned char*)malloc (sizeof (unsigned char)*buffer_size);
    bufferManager.retrieve(posBuffer, retrieved_buffer);
    retrieved_buffers.push_back(retrieved_buffer);
  }

  //compare two results
  for (int i=0; i<totalN; i++) {
    unsigned char *buffer=buffers[i];
    int buffer_size = buffersizes[i];
    unsigned char *bufferc = retrieved_buffers[i];

    size_t hashResult = hash_compute (buffer, buffer_size);
    size_t hashResultc = hash_compute (bufferc, buffer_size);

    EXPECT_EQ  (hashResult, hashResultc);

  }
  
  //free all of the buffers. 
  for (int i=0; i<totalN; i++) {
    unsigned char *buffer=buffers[i];
    unsigned char *bufferc = retrieved_buffers[i];

    free(buffer);
    free(bufferc);
  }

}



TEST(ExtensibleByteBuffersTest, FreeBuffers) {
  int buffer_size=1*1024;
  // Seed with a real random value, if available
  Rand_int randomGenerator_buffersize {1, 10240}; //needs to be at least 1.
  Rand_int randomGenerator_element {0, 240}; //unsigned char
  
  vector <int> buffersizes; 
  vector <unsigned char *> buffers; 
  vector <PositionInExtensibleByteBuffer> posBuffers; 


  int totalN = 100; 

  for (int i=0; i<totalN; i++) {
    int buffer_size =randomGenerator_buffersize();
    //cout << buffer_size <<endl;
    buffersizes.push_back(buffer_size);
    unsigned char *buffer = (unsigned char*)malloc (sizeof (unsigned char)*buffer_size);
    for (int k=0; k<buffer_size; k++) {
        buffer[k] = (unsigned char) randomGenerator_element(); 
    }    
    buffers.push_back(buffer);
  }

  ExtensibleByteBuffers  bufferManager (buffer_size);

  for (int i=0; i<totalN; i++) {
      unsigned char *buffer = buffers[i];
      int buffer_size = buffersizes[i];
      PositionInExtensibleByteBuffer posBuffer = bufferManager.append(buffer, buffer_size); 
      posBuffers.push_back(posBuffer);
  }

  bufferManager.free_buffers();
  int current_buffer_position = bufferManager.current_buffer_position();
  EXPECT_EQ(current_buffer_position, 0);
  //std::vector's operator [] does not provide bound check.
  //EXPECT_THROW(bufferManager.current_buffer(), out_of_range);
  
  //free all of the buffers. 
  for (int i=0; i<totalN; i++) {
    unsigned char *buffer=buffers[i];
    free(buffer);
  }

}

// Step 3. Call RUN_ALL_TESTS() in main().
int main(int argc, char **argv) {
  //with main, we can attach some google test related hooks.
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler() ;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//
// We do this by linking in src/gtest_main.cc file, which consists of
// a main() function which calls RUN_ALL_TESTS() for us.
//
// This runs all the tests you've defined, prints the result, and
// returns 0 if successful, or 1 otherwise.
//
// Did you notice that we didn't register the tests?  The
// RUN_ALL_TESTS() macro magically knows about all the tests we
// defined.  Isn't this convenient?
