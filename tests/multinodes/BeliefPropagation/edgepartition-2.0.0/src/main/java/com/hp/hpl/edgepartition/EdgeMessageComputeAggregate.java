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

package com.hp.hpl.edgepartition;


import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * test for access of unsafe memory 
 */
public class EdgeMessageComputeAggregate {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeMessageComputeAggregate.class.getName());

    //this is a single object.
    public final static EdgeMessageComputeAggregate INSTANCE = new EdgeMessageComputeAggregate();

    private EdgeMessageComputeAggregate () {
        //only one library will be loaded.
        String libraryName = "jniedgepartition";
        initNativeLibrary(libraryName);
    }

    /**
     * to load native shared libraries
     */
    private void initNativeLibrary(String libraryName) {
         try {
             System.loadLibrary(System.mapLibraryName(libraryName));
             LOG.info(libraryName + " shared library loaded via mapLibraryName");
         }
         catch (UnsatisfiedLinkError ex){
             try {
                 NativeLibraryLoader.loadLibraryFromJar("/" + System.mapLibraryName(libraryName));
                 LOG.info(libraryName + " shared library loaded via loadLibraryFromJar");
             }
             catch (IOException exx) {
                 LOG.info("ERROR while trying to load shared library " + libraryName, exx);
                 throw new RuntimeException(exx);
             }
         }
    }

    public native void aggregateComputeMessages(
             int cluster_size, int vertex_size, int localSrcIds_size, 
             long unsafe_index, long vertexAttrs, long localSrcIds, 
             long localDstIds, long data, long aggregates);

}
