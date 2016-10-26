
For the latest online version of the README.md see:
    
  https://github.com/HewlettPackard/sparkle/blob/master/README.md

#About

This package contains a number of Hewlett Packard Labs developed performance enhancements to the Apache Spark project which is available at https://github.com/apache/spark and https://spark.apache.org/.

The contents of this repository are obviously not an entire redistribution of Spark. They are simply the files that we have modified to achieve significant performance enhancements for memory-bandwidth intensive workloads on the Spark 1.2 release.

Hewlett Packard Labs is making these changes available under the same license as Spark - the Apache 2.0 License - and is collaborating with HortonWorks to make these enhancements available in the Spark upstream distribution and through their Enterprise Spark at Scale offering. More information on the HortonWorks/Hewlett Packard Labs collaboration is available at http://hortonworks.com/press-releases/hortonworks-hpe-accelerate-spark/.

#Contributors

For a list of contributors see [AUTHORS](https://github.com/HewlettPackard/sparkle/blob/master/AUTHORS).

#Brief history

Hewlett Packard Labs and HortonWorks have recently deepened their collaboration to further drive Apache Spark innovation. As part of the relationship, Hewlett Packard Labs and Hortonworks will work on key performance improvements for Spark together within the open source community, and integrate these new components/capabilities into the HDP platform for the benefit of new and existing customers and users.
The performance enhancements community are:
* Dramatically improved shuffle execution within Spark which enables a better performance across many workloads and all platforms.
* Improved memory density utilization with Spark so that larger data sets can be processed in the same memory footprint, which enables better scalability and brand new use cases simultaneously.

This collaboration between Hortonworks and Hewlett Packard Labs indicates our mutual support for the growing spark community and solutions. Our ongoing investments will continue to focus on accelerating Spark adoption at scale, integration of Spark into broad data architectures supported by Apache Yarn, enhancements to Spark for performance and better access points for spark application like Apache Zeppelin.

# Quick Start 

In the "docs" directory, the "userguide.pdf" describes how to compile the Spark High-Performance-Computing (HPC) package on a 
multicore big-memory machine and run the applications, under Spark 2.0.  The "exampleapps.pdf" describes how to run example 
applications with specific configuration parameters to be added. The "httpserver-fix.pdf" describes the Jetty HTTP server related 
threading issue on a multicore big-memory machine (e.g., larger than 80 cores) and provides the fix of the issue. 


#License

    The code in this repository is licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

#Copyright

	(c) Copyright 2016 Hewlett Packard Enterprise Development LP

**NOTE**: This software depends on other packages that may be licensed under different open source licenses.

