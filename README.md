## Introduction

Jaccson is a JSON interface for Accumulo. It allows users to insert JSON documents, update JSON documents, index specific fields, and query collections using various operators.

The design is intended to be similar to MongoDB's API. The advantage to using Jaccson over MongoDB is that Accumulo offers simpler administration of large 'sharded' instances, compatibility with Hadoop, better concurrency (no global write lock), and different performance characteristics.

Index building utilizes MapReduce. Updates are performed using Accumulo's iterator functionality, meaning, updates are applied lazily at read / compaction time. This probably results in much higher update and insertion rates, as Accumulo averages 10s of thousands of writes / updates per second this way.

Queries take advantage of separate secondary index tables, built through the ensureIndex() command and maintained thereafter. One particular issue right now is how to inform all open connections of the existence of a new index, since indicies are maintained by the Jaccson client code, i.e. not just on the server. One possible solution would be to store the list of indices in ZooKeeper so updates cause a cache miss to new writes and the new index is then discovered by all connections wishing to write to the table of interest.

Queries also use BatchScanners where necessary to improve performance / simplify the code.

Rather than implement drivers in various languages, as Mongo has done, non JVM language support is planned to be provided via a thrift proxy. Mongo users are accustomed to running separate client-side mongos processes to connect to sharded instances, and so shouldn't have any problem with having to run a proxy. This is only required for non JVM languages however, and Java, Scala, Jython, JRuby, Groovy, Clojure, etc should be able to use the Java client library.

## Status

This code is currently alpha. In particular, all exceptions are currently thrown, and exception handling hasn't been thought through yet.

Most of the initial design is complete, meaning that ways have been found of accomplishing everything needed to replicate Mongo's functionality over Accumulo. Not all of these designs have been tested for correctness or performance yet, and so are subject to change. 

Currently, the design relies on the JSON java library. It may be beneficial to switch to Mongo.org's BSON library for underlying serialization of JSON onto disk. Currently Jaccson serializes JSONObjects to JSON Strings. Accumulo provides built-in compression, however, which somewhat mitigates the cost of storing the longer form JSON.

The bulk of the testing remains to be written for the update code.

## Implementation details

JSON documents are stored in as strings in the Value of the Accumulo table. New documents are assigned a UUID unless the user provides a value for the _id field. The layout of the table is as follows:

RowID: UUID / value of _id field

**Column Family**: "JSON"

**Column Qualifier**: ""

**Column Visibility**: value of $security field, if present

**Timestamp**: Long.MAX_VALUE - current milliseconds (this is necessary to apply updates properly)

**Value**: JSON document as UTF8 string


### Iterators

Jaccson uses three iterators to perform updates, deletes, and selection for JSON documents. 

**JaccsonUpdater**: This iterator looks for special update operators within versions of a particular JSON document and applies them to produce the latest version.

If we have the following versions for a JSON document:

    {'a':1, 'b':2}
    {'$set' {'a': 5}}

The updater will deserialize the first document into a JSONObject and then apply the $set operator to the object, which sets the value of the field 'a' to 5. The updater then deserializes the doc into a string {'a':5, 'b':2} and returns it to the user. If a version of the document appears without any update operators it is considered an overwrite. For these versions:

    {'a':1, 'b':2}
    {'$set' {'a': 5}}
    {'a':9}

The updater will simply return {'a':9} and consider the other versions overwritten. For

    {'a':1, 'b':2}
    {'$set' {'a': 5}}
    {'a':9}
    {'a':6}

The updater will just return the latest, {'a':6}

If the first version of a document contains an update operator, the resulting object depends on the update operator. If the first version encountered is:

    {'$incr': {'a': 1}}

The resulting object will simply be {'a': 1} - that is, incrementing a non-existent field assumes the initial value is 0.


**DeletedFilter**: This iterator looks for special delete marker and ignores older versions of the doc. If we have the following versions for a doc:

    {'a':1, 'b':2}
    {'$set' {'a': 5}}
    {'$delete':1}

The delete filter will simply not return anything for this document. During minor and major compaction, versions of a doc that appear before the delete operator, and the delete operator itself, will be omitted from the files on disk.

If there are versions of a doc after the delete marker, they will still appear in results.

    {'a':1, 'b':2}
    {'$set' {'a': 5}}
    {'$delete':1}
    {'a': 3'}
    
Returns {'a':3}

**Selector**: This iterator applies selection filters documents to produce subdocuments. Given the document:

    {'a':'hello', 'refs':{'r':'j', 's':'k'}}
    
and the selection filter

    {'a':1, 'refs.s':1}
    
The selector will produce the document:

    {'a':'hello', 'refs':{'s':'k'}}

And for the selection filter

    {'refs.r':1}

The selector will produce the subdocument:

    {'refs':{'r':'j'}}


### Indexing

Jaccson can produce indexes for fields within documents. Each index is stored in a separate Accumulo table. The naming convention used for index tables is as follows:

For a table name 'mytable', and an index of the field 'name', the index table will be called 'mytable_name'. An index on the field 'author.location' would be 'mytable_author_location'

When a user invokes collection.ensureIndex() jaccson marks the field as indexed in a jaccson_metadata table in Accumulo, and kicks off a MapReduce job to index any existing data. When clients write to a table they first get a list of indexed fields from the jaccson_metadata table. Clients that are running while another client creates an index need to be informed of the newly indexed field so they can begin indexing those fields at insert time as well. The current design proposal is to make the list of indexed fields available through ZooKeeper so clients can learn immediately of newly indexed fields. 

The MapReduce job will need to take the timestamps of existing data into account when writing entries to the index so that newer index entries written by clients before the MapReduce completes will still appear in the right order, ensuring index consistency. While the index is being built by MapReduce, queries may not return all results, but after the MapReduce completes the index is kept consistent by clients. 

An index can be dropped quickly. Jaccson just deletes the table associated with the index and removes the field from the list of indexed fields for that table.


### Queries

Jaccson implements the MongoDB query language. Queries take advantage of all indexes available, rather than just one as Mongo does. A query builds a list of scanners over index tables, and also will apply filtering to the result set when no index is available. Results are delivered to the user by streaming key value pairs from these scanners when a document fully satisfies the query logic. 

Note that this design means that it is possible to stream lots of documents over the network that will not be delivered to the user, as in the case when doing a query on a field without indexes.



### Drivers
Rather than implement drivers in various languages, as Mongo has done, non JVM language support is planned to be provided via a thrift proxy. Mongo users are accustomed to running separate client-side mongos processes to connect to sharded instances, and so shouldn't have any problem with having to run a proxy. This is only required for non JVM languages however, and Java, Scala, Jython, JRuby, Groovy, Clojure, etc should be able to use the Java client library.

