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

import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.firesteel.shuffle.MapShuffleStore;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * To Simulated Map Shuffle Store. For unit testing purpose.
 */
public class SimulatedMapSHMShuffleStore  implements MapShuffleStore {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatedMapSHMShuffleStore.class.getName());


    public  SimulatedMapSHMShuffleStore(Kryo kryo, ByteBuffer byteBuffer,
                                         SimulatedShuffleStoreManager shuffleStoreManager) {

    }

    @Override
    public void initialize  (int shuffleId, int mapTaskId, int numberOfPartitions,
                 ShuffleDataModel.KValueTypeId keyType, int sizeOfBatchSerialization,
                 boolean ordering) {
        LOG.info("Map-side shared-memory based shuffle store started.");
    }

    @Override
    public void stop() {
        LOG.info("Map-side shared-memory based shuffle store stopped with id:");

    }

    @Override
    public void shutdown() {
        LOG.info("Map-side shared-memory based shuffle store shutdown with id:");

    }

   
    @Override
    public ShuffleDataModel.MapStatus sortAndStore() {
      return null;
    }

    @Override
    public ShuffleDataModel.KValueTypeId getKValueTypeId() {
        throw new UnsupportedOperationException("not yet implemented.");
    }


    @Override
    public void setKValueTypeId(ShuffleDataModel.KValueTypeId ktypeId) {

    }

    @Override
    public void storeVValueType(Object Vvalue) {

    }

    @Override
    public void storeKVTypes (Object Kvalue, Object Vvalue){

    }

    @Override
    public byte[] getKValueType() {
        return null;
    }

    @Override
    public  byte[] getVValueType() {
        return null;
    }

    @Override
    public int getStoreId() {
    	return 0; 
    }

	@Override
	public void serializeKVPair(Object kvalue, Object vvalue, int partitionId,
			int indexPosition, int scode) {
		 throw new UnsupportedOperationException("not yet implemented.");
	}

	@Override
	public void storeKVPairs(int numberOfPairs, int scode) {
		 throw new UnsupportedOperationException("not yet implemented.");
		
	}
}
