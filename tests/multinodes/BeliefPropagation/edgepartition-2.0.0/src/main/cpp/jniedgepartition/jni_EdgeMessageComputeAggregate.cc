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

#include "com_hp_hpl_edgepartition_EdgeMessageComputeAggregate.h"
#include <string.h>
#include <cmath>
#include <stdint.h>
/*
 *  * Class:     com_hp_hpl_edgepartition_EdgeMessageComputeAggregate
 *   * Method:    aggregateComputeMessages
 *    * Signature: (IIIJJJJJJ)V
 *     */
JNIEXPORT void JNICALL Java_com_hp_hpl_edgepartition_EdgeMessageComputeAggregate_aggregateComputeMessages
  (JNIEnv *env, jobject obj, jint cluster_size, jint vertex_size, jint localSrcIds_size, jlong unsafe_index, jlong vertexAttrs, jlong localSrcIds, jlong localDstIds, jlong data, jlong aggregates) {



    // initialize aggregated message to zero
    //printf("cluster_size[%d]\n", cluster_size);
    //printf("vertex_size[%d]\n", vertex_size);
    //printf("localSrcIds_size[%d]\n", localSrcIds_size);
    memset((double*)aggregates, 0, 16L*vertex_size);

    int i=0;
    while(i < cluster_size) {

	// get src and dst ids
        //long clusterSrcId = *(long*)(unsafe_index+(12L)*i); 
    //printf("clusterSrcId[%d]\n", clusterSrcId);
    //**** prefetch next clusterPos
        int clusterPos = *(int*)(unsafe_index+4L*i); 
    //printf("clusterPos[%d]\n", clusterPos);
        int clusterLocalSrcId = *(int*)(localSrcIds+4L*clusterPos); 

        double srcAttr3 = *(double*)(vertexAttrs+40L*clusterLocalSrcId+16L);
    //printf("srcAttrs3[%f]\n", srcAttr3);
        double srcAttr4 = *(double*)(vertexAttrs+40L*clusterLocalSrcId+24L);
    //printf("srcAttrs4[%f]\n", srcAttr4);
	int pos = clusterPos;
        //_mm_prefetch((int*)(localSrcIds+4L*pos),_MM_HINT_T0);
	//_mm_prefetch((int*)(localDstIds+4L*pos),_MM_HINT_T0);
	//_mm_prefetch((double*)(data+64L*pos),_MM_HINT_T0);
	while(pos < localSrcIds_size &&  *(int*)(localSrcIds+4L*pos)  == clusterLocalSrcId) { 

	    //_mm_prefetch((int*)(localSrcIds+4L*(pos+2)),_MM_HINT_T0);
	    //_mm_prefetch((int*)(localDstIds+4L*(pos+2)),_MM_HINT_T0);
	    //_mm_prefetch((double*)(data+64L*(pos+2)),_MM_HINT_T0);

    	    int localDstId = *(int*)(localDstIds+4L*pos); 
    //printf("localDstId[%d]\n", localDstId);

            // get vertex attributes for src and dst
            double dstAttr3 = *(double*)(vertexAttrs+40L*localDstId+16L);
    //printf("dstAttrs3[%f]\n", dstAttr3);
            double dstAttr4 = *(double*)(vertexAttrs+40L*localDstId+24L);
    //printf("dstAttrs4[%f]\n", dstAttr4);

	    // get edge attributes
	    double attr[8];
            //for(int j=0;j<8;j+=1) { 
            attr[0] = *(double*)(data+64L*pos);
            attr[1] = *(double*)(data+64L*pos+8L);
            attr[2] = *(double*)(data+64L*pos+16L);
            attr[3] = *(double*)(data+64L*pos+24L);
            attr[4] = *(double*)(data+64L*pos+32L);
            attr[5] = *(double*)(data+64L*pos+40L);
            attr[6] = *(double*)(data+64L*pos+48L);
            attr[7] = *(double*)(data+64L*pos+56L);
    //printf("attr[%d][%f]\n", j,attr[j]);
 	    //}

            double curMessage00 = attr[0]*srcAttr3/attr[5] +
              attr[1]*srcAttr4/attr[7];
            double curMessage10 = attr[2]*srcAttr3/attr[5] +
              attr[3]*srcAttr4/attr[7];
            double curMessage01 = attr[0]*dstAttr3/attr[4] +
              attr[2]*dstAttr4/attr[6];
            double curMessage11 = attr[1]*dstAttr3/attr[4] +
              attr[3]*dstAttr4/attr[6];

	    // normalize messages
            double sum = curMessage00 + curMessage10;
            curMessage00 = curMessage00/sum;
            curMessage10 = curMessage10/sum;

            double sum1 = curMessage01 + curMessage11;
            curMessage01 = curMessage01/sum1;
            curMessage11 = curMessage11/sum1;

	    // set current messages
    	    *(double*)(data+64L*pos+32L) = curMessage00;
    	    *(double*)(data+64L*pos+40L) = curMessage01;
    	    *(double*)(data+64L*pos+48L) = curMessage10;
    	    *(double*)(data+64L*pos+56L) = curMessage11;
    //printf("curMessage11[%f][%f]\n", *(double*)(data+8L*8*pos+8L*7), curMessage11);

	    // get aggregated messages
	    double src_aggregates[2];
	    double dst_aggregates[2];
	    for(int j=0;j<2;j++) {
                src_aggregates[j] = *(double*)(aggregates+16L*clusterLocalSrcId+8L*j);
                dst_aggregates[j] = *(double*)(aggregates+16L*localDstId+8L*j);
    //printf("dst_aggregates[%d][%f]\n", j,dst_aggregates[j]);
	    }
            // set aggregated messages
	    *(double*)(aggregates+16L*clusterLocalSrcId) = src_aggregates[0] + std::log(curMessage01);
	    *(double*)(aggregates+16L*clusterLocalSrcId+8L) = src_aggregates[1] + std::log(curMessage11);
	    *(double*)(aggregates+16L*localDstId) = dst_aggregates[0] + std::log(curMessage00);
	    *(double*)(aggregates+16L*localDstId+8L) = dst_aggregates[1] + std::log(curMessage10);
    //printf("aggregates[%f][%f]\n",  *(double*)(aggregates+8L*2*localDstId+8L), (dst_aggregates[1] + std::log(curMessage10)));
            pos++;	
	}
	i++;
    }
}

