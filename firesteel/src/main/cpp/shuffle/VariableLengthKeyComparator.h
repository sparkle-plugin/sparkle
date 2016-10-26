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

#ifndef VARABILE_LENGTH_KEY_COMPARATOR_H_
#define VARABILE_LENGTH_KEY_COMPARATOR_H_

#include "ExtensibleByteBuffers.h"

class VariableLengthKeyComparator {
 public: 

  //to compare the two byte arrays that are cached in local cache manager.
  static bool less_than (ExtensibleByteBuffers *bufferMgr,
				    const PositionInExtensibleByteBuffer &a,
			 const PositionInExtensibleByteBuffer &b){

     bool comp_result=false;
     //get to the actual key (start from the beginning), we advance char by char, until it is done.
     //we may need to get only the first several chars in most cases.
     bool done=false;
   
     int a_buffer=a.start_buffer;
     int a_position=a.position_in_start_buffer;
     int a_scanned = 0;

     int b_buffer=b.start_buffer;
     int b_position=b.position_in_start_buffer;
     int b_scanned = 0;

     int a_buffer_capacity=bufferMgr->buffer_at(a_buffer).capacity();
     int b_buffer_capacity=bufferMgr->buffer_at(b_buffer).capacity();
    
     while (!done) {
       unsigned char va = bufferMgr->buffer_at(a_buffer).value_at(a_position);
       unsigned char vb = bufferMgr->buffer_at(b_buffer).value_at(b_position);
       a_scanned++;
       b_scanned++;

       //we get the winner.
       if (va < vb) {
  	  done = true;
	  comp_result=true;
       }
       else if (va > vb) {
	 done = true;
	 comp_result=false;
       }
       else {
         //need to advance to next character.
	 //one of them exhausted, the other still have more
	 if ((a_scanned ==a.value_size) 
		  || (b_scanned == b.value_size)){
	   int a_remained = a.value_size-a_scanned;
	   int b_remained = b.value_size-b_scanned;
	 
           if (a_remained <b_remained) {
		 //b wins
		 done = true;
		 comp_result=true;
	   }
	   else {
	        //they are truely identical or a wins, in either case,
		done = true;
		comp_result = false;
	   }
	 }
         else {

             //both can advance to next char
	     a_position++;
	     b_position++;
        
	    if (a_position == a_buffer_capacity) {
  	       a_buffer++;
	       a_position=0;
	    }
         
	    if (b_position == b_buffer_capacity) {
  	       b_buffer++;
	       b_position=0;
	    }
	 }

       }//end else
  
     }//end while 
   
     return comp_result;

  }

  //to scan the byte arrays, to decide whether they are equal.
  static bool is_equal (ExtensibleByteBuffers *bufferMgr,
				    const PositionInExtensibleByteBuffer &a,
			const PositionInExtensibleByteBuffer &b) {
     
     //first compare the size, if not the same, return immediately
     if (a.value_size != b.value_size) {
       return false;
     }

     //now the size is identical.
     bool comp_result=false;
     //get to the actual key (start from the beginning), we advance char by char, until it is done.
     //we may need to get only the first several chars in most cases.
     bool done=false;
   
     int a_buffer=a.start_buffer;
     int a_position=a.position_in_start_buffer;
     int scanned = 0;

     int b_buffer=b.start_buffer;
     int b_position=b.position_in_start_buffer;

     int a_buffer_capacity=bufferMgr->buffer_at(a_buffer).capacity();
     int b_buffer_capacity=bufferMgr->buffer_at(b_buffer).capacity();
    
     while (!done) {
       unsigned char va = bufferMgr->buffer_at(a_buffer).value_at(a_position);
       unsigned char vb = bufferMgr->buffer_at(b_buffer).value_at(b_position);
       scanned++;

       //we get the winner.
       if (va != vb) {
  	  done = true;
	  comp_result=false;
       }
       else {
         //need to advance to next character.
	 //both arrays have identical length, when the control reaches here.
	 if (scanned ==a.value_size){
	      done = true;
	      comp_result=true; //advance to the last one, still the same.
	 }
         else {

             //both can advance to next char. but the cursors are different.
	     a_position++;
	     b_position++;
        
	    if (a_position == a_buffer_capacity) {
  	       a_buffer++;
	       a_position=0;
	    }
         
	    if (b_position == b_buffer_capacity) {
  	       b_buffer++;
	       b_position=0;
	    }
	 }

       }//end else
  
     }//end while 
   
     return comp_result;

  }

  static bool is_equal (const unsigned char *buf1, int size1, 
			const unsigned char *buf2, int size2){
    if (size1 != size2) {
      return false;
    }
    else {
      int i = 0;
      while ((i < size1 - 1) && ( buf1[i] == buf2[i] )) {
        i++;
      }
    
      return (buf1[i] == buf2[i]);

    }
 }	
};



#endif /*VARABILE_LENGTH_KEY_COMPARATOR_H_*/
