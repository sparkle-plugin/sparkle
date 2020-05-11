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

package com.hp.hpl.firesteel.shuffle;

/**
 * Some common data structures that go through the map-->shuffle-->reduce
 */
public interface ShuffleDataModel {

    /**
     * To record the Map shuffle information that will be sent to the MapTracker and retrieved
     * by the Reducer later. The information defined in Spark MapStatus include:
     *   *BlockManager ID, which further includes {executor id: string, host: string, port: int}
     *   *Array of Long's, with each Long value corresponding to the size of total bucket size
     * (in bytes?).
     *
     * to Simplify network transportation, what gets sent to the scheduler about MapStatus, is the
     * compressed version, in which each Long value gets compressed via LOG(.).
     */
    public class MapStatus {
        // we will just use today's uncompressed MapStatus for C++ to populate Array of Bucket Size
        // information, and then handle over to the "stop" call the compressed MapStatus defined in
        // SHM-based shuffle writer..
        private long mapStatus[];
        private long regionIdOfIndexBucket; 
        private long offsetOfIndexBucket;
        private long dataChunkWrittenTimeNs;

        public MapStatus() {
            mapStatus = null;
            regionIdOfIndexBucket = -1; 
            offsetOfIndexBucket = -1;
            dataChunkWrittenTimeNs = 0L;
        }

        /**
           @param writtenTime Time spend writing Data Chunk in nano sec.
         */
        public MapStatus (long [] status, int regionId, long offset, long writtenTime)
        {
            this.mapStatus = status;
            this.regionIdOfIndexBucket = regionId;
            this.offsetOfIndexBucket = offset;
            this.dataChunkWrittenTimeNs = writtenTime;
        }

        public long[] getMapStatus () {
            return this.mapStatus;
        }

        public long getRegionIdOfIndexBucket() {
            return this.regionIdOfIndexBucket;
        }

        public long getOffsetOfIndexBucket() {
            return this.offsetOfIndexBucket;
        }

        public long getWrittenTimeNs() {
            return this.dataChunkWrittenTimeNs;
        }
    }

    /**
     * The information allows the C++ shuffle engine to get to the correct shared-memory region to fetch the data
     */
    public class ReduceStatus {
        //the corresponding map id's that contribute data to the reducer.
        private int mapIds[];
        // obtained from MapOutputTracker: for map source, what is the expected size of byte content
        // How can we get into each source is a place holder at this time via shared-memory region.
        private long regionIdsOfIndexChunks[];
        private long offsetsOfIndexChunks[];
        //size of each bucket on the corresponding map.
        private long sizes[];
        // # of data chunks loaded in the store.
        private long numDataChunks = 0L;
        // size of data chunks loaded in the store.
        private long bytesDataChunks = 0L;
        //# of remote data chunks loaded in the store.
        private long numRemoteDataChunks = 0L;
        // size of remote data chunks loaded in the store.
        private long bytesRemoteDataChunks = 0L;
        // # of kvpairs loaded in the store.
        private long numRecords = 0L;

        public ReduceStatus (int mapIds[], long regionIds[], long offsetToIndexChunks[], long sizes[],
                             long numDataChunks, long bytesDataChunks,
                             long numRemoteDataChunks, long bytesRemoteDataChunks,
                             long numRecords) {
            this.mapIds = mapIds;
            this.regionIdsOfIndexChunks =  regionIds;
            this.offsetsOfIndexChunks = offsetToIndexChunks;
            this.sizes = sizes;
            this.numDataChunks = numDataChunks;
            this.bytesDataChunks = bytesDataChunks;
            this.numRemoteDataChunks = numRemoteDataChunks;
            this.bytesRemoteDataChunks = bytesRemoteDataChunks;
            this.numRecords = numRecords;
        }

        public int [] getMapIds() {
            return this.mapIds;
        }
        public long[] getRegionIdsOfIndexChunks () {
            return this.regionIdsOfIndexChunks;
        }

        public long[] getOffsetsOfIndexChunks() {
            return this.offsetsOfIndexChunks;
        }
        public long[] getSizes() {
            return this.sizes;
        }

        public long getNumDataChunks() {
            return this.numDataChunks;
        }
        public long getBytesDataChunks() {
            return this.bytesDataChunks;
        }
        public long getNumRemoteDataChunks() {
            return this.numRemoteDataChunks;
        }
        public long getBytesRemoteDataChunks() {
            return this.bytesRemoteDataChunks;
        }
        public long getNumRecords() {
            return this.numRecords;
        }
    }

    public enum KValueTypeId {
        Int (0),
        Long(1),
        Float(2),
        Double(3),
        String(4),
        ByteArray(5),
        Object(6),
        Unknown(7);

        int state;
        private KValueTypeId (int state) {
            this.state = state;
        }

        public int getState() {
            return this.state;
        }
    }

    public class MergeSortedResult {
        //unify all of the different key types. I only need to populate one of them.
        private int intKvalues[];
        private long longKvalues[];
        private float floatKvalues[];
        private String stringKvalues[];
        //Note that we only need value offsets, as in each value-group, the de-serializer knows how to
        //de-serialize each value object.
        private int voffsets[];
        //for variable keys
        private int koffsets[];

        //to indicate whether the de-serialization buffer is big enough or not to hold this batch of k-vs pairs
        private boolean bufferExceeded;

        private long numLocalBucketsRead = 0;
        private long numRemoteBucketsRead = 0;
        private long bytesLocalBucketsRead = 0;
        private long bytesRemoteBucketsRead = 0;

        public MergeSortedResult (){
            intKvalues= null;
            longKvalues = null;
            floatKvalues = null;
            stringKvalues = null;
            bufferExceeded = false;
        }

        public void setIntKValues(int values[]) {
            this.intKvalues = values;
        }

        public void setLongKValues(long values[]) {
            this.longKvalues = values;
        }

        public void setFloatKValues(float values[]) {
            this.floatKvalues = values;
        }

        public void setStringKValues(String values[]) {
            this.stringKvalues = values;
        }

        //for variable keys
        public void setKoffsets (int offsets[]) {
            this.koffsets = offsets;
        }

        public void setVoffsets (int offsets[]) {
            this.voffsets = offsets;
        }

        public void setBufferExceeded (boolean val) { this.bufferExceeded = val; }

        public int[] getIntKvalues (){
            return this.intKvalues;
        }

        public long[] getLongKvalues() {
            return this.longKvalues;
        }

        public float[] getFloatKvalues() {
            return this.floatKvalues;
        }

        public String[] getStringKvalues() {
            return this.stringKvalues;
        }

        public int[] getKoffsets() {
            return this.koffsets;
        }

        public int[] getVoffsets() {
            return this.voffsets;
        }

        public boolean getBufferExceeded() { return this.bufferExceeded; }

        public long getNumLocalBucketsRead() {
            return this.numLocalBucketsRead;
        }
        public long getNumRemoteBucketsRead() {
            return this.numRemoteBucketsRead;
        }
        public long getBytesLocalBucketsRead() {
            return this.bytesLocalBucketsRead;
        }
        public long getBytesRemoteBucketsRead() {
            return this.bytesRemoteBucketsRead;
        }
    }
    //we can have other types, later.
}
