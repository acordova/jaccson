Jaccson is a JSON interface for Accumulo. It allows users to insert JSON documents, update JSON documents, index specific values, and query collections using various operators.

The design is intended to be similar to MongoDB's API. The advantage to using Jaccson over MongoDB is that Accumulo offers simpler administration of large 'sharded' instances, compatibility with Hadoop, better concurrency (no global write lock), and different performance characteristics.

This code is currently alpha. In particular, all exceptions are currently thrown, and exception handling hasn't been thought through yet.

Most of the initial design is complete, meaning that ways have been found of accomplishing everything needed to replicate Mongo's functionality over Accumulo. Not all of these designs have been tested for correctness or performance yet, and so are subject to change. 

Currently, the design relies on the JSON java library. It may be beneficial to switch to Mongo.org's BSON library for underlying serialization of JSON onto disk. Currently Jaccson serializes JSONObjects to JSON Strings. Accumulo provides built-in compression, however, which somewhat mitigates the cost of storing the longer form JSON.

Index building utilizes MapReduce. Updates are performed using Accumulo's iterator functionality, meaning, updates are applied lazily at read / compaction time. This probably results in much higher update and insertion rates, as Accumulo averages 10s of thousands of writes / updates per second this way.

Queries take advantage of separate secondary index tables, built through the ensureIndex() command and maintained thereafter. One particular issue right now is how to inform all open connections of the existence of a new index, since indicies are maintained by the Jaccson client code, i.e. not just on the server. One possible solution would be to store the list of indices in ZooKeeper so updates cause a cache miss to new writes and the new index is then discovered by all connections wishing to write to the table of interest.

Queries also use BatchScanners where necessary to improve performance / simplify the code.

The bulk of the testing remains to be written for the update code.

Rather than implement drivers in various languages, as Mongo has done, non JVM language support is planned to be provided via a thrift proxy. Mongo users are accustomed to running separate client-side mongos processes to connect to sharded instances, and so shouldn't have any problem with having to run a proxy. This is only required for non JVM languages however, and Java, Scala, Jython, JRuby, Groovy, Clojure, etc should be able to use the Java client library.