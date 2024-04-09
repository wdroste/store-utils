
Goals:

Often times Neo4j gets fragmented and needs to be cleaned.

* Ability to rebuild database
* Ability to rebuild the indexes of the database.

Expected instructions:

* Dump the data and index information for a running database.
* Reset the database
* Restore the data and indexes to the database.


In order to dump the indexes and the database.

Use the IndexManager in this application to record each of the indexes in the current database the name is required as many applications will use that to determine the existence of the index and recreate them. 

In order to dump the database reliably use key pagination. First detect all the labels.

```
call db.labels();
```

NOTE: 
If the node does not have a label this will not work (all nodes should have a label). If the Node does not have an `id` an indexed `id` column pagination will not work.

Iterate over each label grabbing the node's properties. Use Kryo to serialize the map coming from the Neo4j Driver.

```java
node.toMap()
```

Use `speedb` to store the id -> map to file for later retrieval. Those this could just be a file appended prefixed with the size of the map in bytes. However `speedb` offers compaction, compression, and splits the KV store into separate files so its easier to manage.

Use a file per label to make it easier to go back and traverse just the IDs.

Use pagination again with a 2 loops over the combinations of labels to get all the relationships. Use all the `ids` from the file to batch read the relationships to other labels.

```
for (srcLabel in labels) {
   for (destLabel in labels) {
     iter = iterator(label) // batch ids
     read all relationships
     MATCH (s:<srcLabel>)-[r]-(d:<destLabel>)             RETURN s.id as sid, d.id as did, type(r), r",
     // save all this information to a file
   }
}
```

Use a NodeCreate to build out all the simple creates and iterate through each of the labels files.

Use a NodeUpdateDTO to build all the relationship diffs (which are just creates) to create all the relationships in a single thread.


