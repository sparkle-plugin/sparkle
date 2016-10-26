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

#include "ByteBufferPool.h"
#include "SimpleUtils.h"
#include <string.h>
#include <string>
#include <vector>
#include <iostream>
#include <random>
#include <functional>

#include "gtest/gtest.h"
#include "glog/logging.h"

using namespace std;

TEST(ByteBufferPoolTest, SequenceOfAllocAndFreeForOneBuffer) {
 
  LOG(INFO) << "test 1 called SequenceOfALlocAndFreeForOneBuffer" <<endl;

  int poolSize= 4; 
  const int byteBufferSize = 100;
  unsigned char content[byteBufferSize];

  ByteBufferPool  *byteBufferPool = new QueueBasedByteBufferPool(poolSize);

  byteBufferPool->initialize();  
  unsigned char *p0 = byteBufferPool->getBuffer();  
  EXPECT_EQ (p0, nullptr);

  p0 = (unsigned char*) malloc(byteBufferSize); 
  memcpy(p0, content, byteBufferSize);
  //enque one.
  byteBufferPool->freeBuffer(p0); 

  unsigned char * p1 = byteBufferPool->getBuffer();
   
  //what it goes inside, should be the same as going out
  EXPECT_EQ (p1, p0); 
  memcpy(p1, content, byteBufferSize);

  //when done, push it back
  byteBufferPool->freeBuffer(p1);

  int currentSize = byteBufferPool->currentSize();
  EXPECT_EQ (currentSize, 1);

  byteBufferPool->shutdown(); 
}


TEST(ByteBufferPoolTest, SequenceOfAllocAndFreeForMultipleBuffers) {

  int poolSize= 4; 
  const int byteBufferSize = 100;
  unsigned char content[byteBufferSize];

  ByteBufferPool  *byteBufferPool = new QueueBasedByteBufferPool(poolSize);

  byteBufferPool->initialize();  
  unsigned char *p0 = byteBufferPool->getBuffer();  
  EXPECT_EQ (p0, nullptr);

  p0 = (unsigned char*) malloc(byteBufferSize); 
  memcpy(p0, content, byteBufferSize);

  byteBufferPool->freeBuffer(p0); 

  unsigned char * p1 = byteBufferPool->getBuffer();
   
  //what it goes inside, should be the same as going out
  EXPECT_EQ (p1, p0); 
  memcpy(p1, content, byteBufferSize);
  byteBufferPool->freeBuffer(p1); 

  unsigned char *p2 = (unsigned char*) malloc(byteBufferSize); 
  memcpy(p2, content, byteBufferSize);
  byteBufferPool->freeBuffer(p2);

  unsigned char *p3 = (unsigned char*) malloc(byteBufferSize); 
  memcpy(p3, content, byteBufferSize);
  byteBufferPool->freeBuffer(p3);

  unsigned char *p4 = (unsigned char*) malloc(byteBufferSize); 
  memcpy(p4, content, byteBufferSize);
  byteBufferPool->freeBuffer(p4);

  int currentSize = byteBufferPool->currentSize();
  EXPECT_EQ (currentSize, 4);

  unsigned char *p5 = byteBufferPool->getBuffer();
  //the first one getting into the queue.
  EXPECT_EQ (p5, p1);

  byteBufferPool->shutdown(); 
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
