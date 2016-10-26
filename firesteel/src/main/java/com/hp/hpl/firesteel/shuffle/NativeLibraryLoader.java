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

import java.nio.file.Path; 
import java.nio.file.Paths; 
import java.nio.file.Files; 
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

import java.io.IOException; 

/**
 * The utility to extract and load shared library from the packaged JAR file
 */
public class NativeLibraryLoader {
        //if we need to profile the native shared library, the library should have a fixed name for
	//the profiler to be attached to. 
	public static boolean ATTACHED_TO_PROFILER = false;
	
	/**
	 * To load the specified shared library from the current JAR package 
	 * @param libName: the name is assumed to be in the format of "/a/b/xyz.so". 
	 */
	public static void loadLibraryFromJar(String libName) throws IOException {
		if (libName == null) {
			throw new IllegalArgumentException ("library name to be loaded cannot be null.");
		}
		try {
			System.loadLibrary(libName);
		}
		catch (UnsatisfiedLinkError ex) {
		    Path path = Paths.get(libName);
		    boolean withAbsolutePath = path.startsWith("/");
		    if (!withAbsolutePath) {
		      throw new IllegalArgumentException (
			 "library to be loaded:" + libName + " needs to start with /");
		    }

		    Iterator<Path> paths = path.iterator();
			
		    Path lastOne=null;
		    while (paths.hasNext()){
				lastOne = paths.next();	
		    }
		    if (lastOne==null) {
			throw new IllegalArgumentException (
				 "library to be loaded:" + libName + " should not have last path element to be null");
		    }
			
		    String fileName = lastOne.toString(); 
			
		    int index = fileName.lastIndexOf(".");
		    String fileNameWithoutExtension = fileName.substring(0, index);
		    String fileNameExtension = fileName.substring(index, fileName.length());
			
		    File tempFile = null;
		    if (ATTACHED_TO_PROFILER) {
		          tempFile = new File ("/tmp/" + fileName);
		          if (!tempFile.createNewFile()) {
		              throw new FileNotFoundException (
		               "File " + tempFile.getAbsolutePath() + " cannot be created for library loading.");
		          }
		    }
		    else {
			   tempFile = File.createTempFile(fileNameWithoutExtension,  fileNameExtension);
		    }
			
		    tempFile.deleteOnExit(); 
		    
		    Path destPath = tempFile.toPath();
		    
		    //now copy the library to the temporary file 
		    InputStream inStream = NativeLibraryLoader.class.getResourceAsStream(libName);
		    if (inStream == null) {
		    	throw new FileNotFoundException (libName + " cannot be extracted from current JAR file");
		    }
		    
		    Files.copy(inStream, destPath, StandardCopyOption.REPLACE_EXISTING);
		    
		    //set the copied file to be readable and executable 
		    boolean success = ( tempFile.setReadable(true) ) && 
		    		             (tempFile.setExecutable(true));
		    if (!success) {
		    	throw new IOException (
		    	  "transient shared library file: " + destPath + " cannot have executable attribute set");
		    }
		   
		    //finally
		    System.load (destPath.toString());
		}
	}
}
