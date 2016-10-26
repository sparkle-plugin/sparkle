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

package com.hp.hpl.firesteel.shuffle.simulated;

import com.hp.hpl.firesteel.shuffle.ShuffleDataModel;

import com.esotericsoftware.kryo.Kryo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * Simulated Shuffle Store Manager. For unit testing purpose.
 */
public class SimulatedShuffleStoreManager {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatedShuffleStoreManager.class.getName());
    private boolean initialized=false;

    //this is a single object.
    public final static SimulatedShuffleStoreManager INSTANCE = new SimulatedShuffleStoreManager();

    private SimulatedShuffleStoreManager () {
        //the private constructor, so that
    }

    /**
     * NOTE: this will have to be done by the worker node initialization
     * to initilize per-executor shuffle store
     * @return
     */
    public synchronized SimulatedShuffleStoreManager initialize() {
        if (!initialized) {
            initialized = true;
        }

        return this;
    }


    public SimulatedMapSHMShuffleStore createMapShuffleStore(Kryo kryo, ByteBuffer byteBuffer,
                                  int shuffleId, int mapId, int numberOfPartitions,
                                  ShuffleDataModel.KValueTypeId keyType, int sizeOfBatchSerialization,
                                  boolean ordering) {
        SimulatedMapSHMShuffleStore mapShuffleStore=
                      new SimulatedMapSHMShuffleStore(kryo, byteBuffer, this);
        mapShuffleStore.initialize(shuffleId, mapId, numberOfPartitions, keyType, sizeOfBatchSerialization, ordering);
        return mapShuffleStore;
    }


    public SimulatedReduceSHMShuffleStore createReduceShuffleStore(Kryo kryo, ByteBuffer byteBuffer,
                                                          int shuffleId, int reduceId, int numberOfPartitions,
                                                          boolean ordering, boolean aggregation) {
        SimulatedReduceSHMShuffleStore reduceShuffleStore=
                new SimulatedReduceSHMShuffleStore (kryo, byteBuffer, this);
        reduceShuffleStore.initialize(shuffleId, reduceId, numberOfPartitions, ordering, aggregation);
        return reduceShuffleStore;
    }
}
