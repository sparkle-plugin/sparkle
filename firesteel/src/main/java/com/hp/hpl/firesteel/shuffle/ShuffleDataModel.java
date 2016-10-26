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

        public MapStatus() {
            mapStatus = null;
            regionIdOfIndexBucket = -1; 
            offsetOfIndexBucket = -1;
        }

        public MapStatus (long [] status, int regionId, long offset)
        {
            this.mapStatus = status;
            this.regionIdOfIndexBucket = regionId;
            this.offsetOfIndexBucket = offset;
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

        public ReduceStatus (int mapIds[], long regionIds[], long offsetToIndexChunks[], long sizes[])  {
            this.mapIds = mapIds;
            this.regionIdsOfIndexChunks =  regionIds;
            this.offsetsOfIndexChunks = offsetToIndexChunks;
            this.sizes = sizes;
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
    }

    //we can have other types, later.
}
