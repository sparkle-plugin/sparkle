
# About
This package contains a number of performance enhancements to the [Apache Spark](https://github.com/apache/spark) for large-memory NUMA systems.

The contents of this repository are obviously not an entire redistribution of Spark. They are simply the files that we have modified to achieve significant performance enhancements for memory intensive workloads.

# Brief history
As part of The Machine project, Hewlett Packard Labs modified Spark 1.2 to run large memory workloads that used map-reduce operations. The focus within The Machine project was on architectures that enable memory to be shared across compute nodes. In such an environment, two changes have the potential to significantly improve performance:
* Since memory is shared across nodes, the shuffle operation can use shared memory operations rather than doing copies across a TCP/IP connection.
* Since memory is large, JVM limitations associated with gargbage collection and memory management can be significantly improved by creating RDDs using a separate memory manager.

Sparkle includes code to augment Spark with these changes. While the target environment within The Machine project assumed that memory could be shared across nodes, these changes are useful even with conventional architectures whenever large data sets need to be handled within Spark and/or when it is possible to run multiple Spark executors on the same machine.

# Known limitations
The original project was intended to simply prove out the value of The Machine architecture, not to implement all features within Spark. We are in the process of testing the implementation on large-memory machines, and adding functionality as we find gaps. We welcome additional contributions and bug-fixes from users.

# Quick start 
In the "docs" directory, the "userguide.pdf" describes how to compile the Spark High-Performance-Computing (HPC) package on a 
multicore big-memory machine and run the applications, under Spark 2.0.  The "exampleapps.pdf" describes how to run example 
applications with specific configuration parameters to be added. The "httpserver-fix.pdf" describes the Jetty HTTP server related 
threading issue on a multicore big-memory machine (e.g., larger than 80 cores) and provides the fix of the issue.

# Installation and testing
In the "docs" directory, the "userguide.pdf" describes how to compile the Spark High-Performance-Computing (HPC) package on a 
multicore big-memory machine and run the applications, under Spark 2.0.  The "exampleapps.pdf" describes how to run example 
applications with specific configuration parameters to be added. The "httpserver-fix.pdf" describes the Jetty HTTP server related 
threading issue on a multicore big-memory machine (e.g., larger than 80 cores) and provides the fix of the issue. 

# License

    The code in this repository is licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# Copyright

	(c) Copyright 2016 Hewlett Packard Enterprise Development LP

**NOTE**: This software depends on other packages that may be licensed under different open source licenses.

