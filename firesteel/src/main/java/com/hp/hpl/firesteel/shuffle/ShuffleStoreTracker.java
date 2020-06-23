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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is per executor tracker on which shuffle id covers which map tasks on each executor, for a single job. 
 */
class ShuffleStoreTracker {

    private final int DEFAULT_INITIAL_CAPACITY = 8;
    private int initialCapacity = DEFAULT_INITIAL_CAPACITY;

    //the tracker for all of the shuffle Ids. Key is the shuffle Id, value is the thread-based-tracker.
    private ConcurrentHashMap<Integer, ThreadBasedTracker> shuffleTracker;

    /**
     * per executor. To anticipate some task execution failure that the per-thread resource (in this case,
     * the logical thread id), did not return correctly to shuffle resource tracker.
     */
    public ShuffleStoreTracker (int initialCapacity) {
        this.shuffleTracker = new ConcurrentHashMap<Integer, ThreadBasedTracker>();
        this.initialCapacity = initialCapacity;
    }

    /**
     * per worker node
     */
    public void shutdown() {
        Collection<ThreadBasedTracker> c = this.shuffleTracker.values();

        for(ThreadBasedTracker tracker: c) {
            tracker.clear();
        }

        this.shuffleTracker.clear();
    }

    /**
     * put the created map shuffle store into the  tracker, using the shuffle id as the hash map key.
     * and the logical id as the entry to get to the per-thread tracker.
     * Note: the tracker only handles the map-side tracker. There is no reducer-side NVM resources that we need to do
     * clean-up.
     * @param shuffleId: the shuffle id that the map shuffle store belongs to
     * @param logicalThreadId: the thread which has a unique logical thread id, assigned by the shuffle store manager.
     * @param store: the map shuffle store that is to be put into the tracker.
     */
    public void addMapShuffleStore(int shuffleId, int logicalThreadId, MapSHMShuffleStore store) {
        //this returns the previous value.
        ThreadBasedTracker threadBasedTracker =
            this.shuffleTracker.putIfAbsent(Integer.valueOf(shuffleId),
                                            new ThreadBasedTracker(this.initialCapacity));
        if (threadBasedTracker == null) {
            //if the previous value is null, this should retrieve the non-null value. 
            threadBasedTracker = this.shuffleTracker.get(Integer.valueOf(shuffleId));
        }

        threadBasedTracker.add(logicalThreadId, store);
    }

    /**
     * return the map shuffle store that belongs to the specified shuffle id, in the corresponding executor. 
     * @param shuffleId the specified shuffle id for querying the tracker database 
     * @return return the array list that contains the identified map shuffle store; or null, if nothing can be found.
     */
    public ArrayList<MapSHMShuffleStore> getMapShuffleStores(int shuffleId) {
        ArrayList<MapSHMShuffleStore> retrievedStore = null;
        ThreadBasedTracker threadBasedTracker =
            this.shuffleTracker.get(Integer.valueOf(shuffleId));
        if (threadBasedTracker!=null) {
            retrievedStore =  threadBasedTracker.gatherShuffleStore();
        }

        return retrievedStore;
    }

    public void removeMapShuffelStores (int shuffleId) {
        ThreadBasedTracker threadBasedTracker =
            this.shuffleTracker.remove(Integer.valueOf(shuffleId));
        if (threadBasedTracker != null) {
            threadBasedTracker.clear();
        }
    }

    /**
     * on each thread, the map shuffle stores ever run on this thread, for a given shuffle id. 
     *
     */
    private static class PerThreadShuffleStoreTracker{
        private  ArrayList <MapSHMShuffleStore>  mapStoresInShuffle;

        public  PerThreadShuffleStoreTracker () {
            this.mapStoresInShuffle = new ArrayList<MapSHMShuffleStore> ();
        }

        public void track (MapSHMShuffleStore store) {
            this.mapStoresInShuffle.add(store);
        }

        public ArrayList <MapSHMShuffleStore>  retrieve() {
            return this.mapStoresInShuffle;
        }

        public void clear() {
            this.mapStoresInShuffle.clear();
        }

        public int size() {
            return this.mapStoresInShuffle.size();
        }

        /**
         * add to the retrieved store, the map shuffle stores held in this per-thread shuffle store tracker.
         * @param retrievedStore
         */
        public void gatherShuffleStore(ArrayList<MapSHMShuffleStore> retrievedStore) {
            retrievedStore.addAll(this.mapStoresInShuffle);
        }
    }

    /**
     * This tracker contains the data structure of an array, with the size of the entries specified for the 
     * maximum concurrent number of the task threads in the array. For each entry, it is an array list 
     * that contains each entry with <map id, MapSHMShuffleStore>, even happens to that task thread in the same
     * executor.
     * Each shuffle id has one thread-based-tracker to be associated with.
     */
    private static class ThreadBasedTracker {
        //the SimpleEntry has the pair of the map id, and the corresponding reference to MapSHMShuffleStore
        private ArrayList<PerThreadShuffleStoreTracker> slots;
        private boolean cleared = false;

        /**
         * This is invoked when the shuffle store manager is started.
         * @param initialCapacity: the initial number of logical threads corresponding slots.
         */
        public  ThreadBasedTracker(int initialCapacity) {
            //Constructs an empty list with the specified initial capacity.
            this.slots = new ArrayList<>(initialCapacity);

            for (int i=0; i<this.slots.size(); i++) {
                this.slots.add(null);
            }
        }

        /**
         * clear all of the map-shufle store being tracker across each thread, and for all the threads. 
         */
        public void clear() {
            synchronized (this) {
                for (int i=0; i<this.slots.size(); i++) {
                    PerThreadShuffleStoreTracker tracker = this.slots.get(i);
                    if (tracker != null) {
                        tracker.clear();
                    }
                }

                this.slots.clear();
                this.cleared = true;
            }
        }

        public void add(int logicalThreadId,  MapSHMShuffleStore store) {
            // Consider thread timeout and task failure in the Executor process
            // which increase logicalThreadId.

            synchronized (this) {
                if (this.cleared) {
                    // This shuffle has already been unregistered.
                    return ;
                }

                while (this.slots.size() <= logicalThreadId) {
                    this.slots.add(null);
                }

                PerThreadShuffleStoreTracker perThreadTracker =
                    this.slots.get(logicalThreadId);
                if (perThreadTracker == null) {
                    //create the entry in a lazy way
                    perThreadTracker = new PerThreadShuffleStoreTracker();
                    this.slots.set(logicalThreadId, perThreadTracker);
                }

                //use the non-null per-thread-tracker to track the store.
                perThreadTracker.track(store);
            }
        }

        public ArrayList<MapSHMShuffleStore> gatherShuffleStore() {
            ArrayList<MapSHMShuffleStore> retrievedStore = new ArrayList<>();

            synchronized (this) {
                if (this.cleared) {
                    return retrievedStore;
                }

                for (int i=0; i<this.slots.size(); i++) {
                    PerThreadShuffleStoreTracker tracker = this.slots.get(i);
                    if ( (tracker != null) && (tracker.size() > 0)){
                        tracker.gatherShuffleStore(retrievedStore);
                    }
                }
            }

            return retrievedStore;
        }
    }
}
