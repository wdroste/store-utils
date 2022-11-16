# Store-Utils Neo4j Database Compaction and Index Maintenance

This project was inspired by the work of Michael Hunger in the *store-utils* and so its a fork, mostly a rewrite of that effort.  Its also a continuation 
for the 4.x community edition.

## Use Cases:

The use cases form around either compaction/optimization for a long running database or optimization after a large deletion. Neo4j will attempt to recover space, but often it's just better to de-fragment or compact the database to restore it to optimal performance.

Examples:
* Overrun of data that was deleted and now the database is abnormally large.
* Large deletion required to clean the database of no longer used data.

NOTE: 
It's generally better to label nodes to be deleted and the use this tool to do the actual deletion in a maintenance window. For large scale deletions, for 
those that well over 10 million nodes.

## Overview

There are basically 4 parts to the optimization process.

* Determine the existing indexes in the data
* Optimize the data, copy source to target filtering out nodes to delete
* Add the users to the new copy of the database
* Rebuilding the indexes in the target copy


## Preparation

* There must be sufficient space to copy the database to a target directory. The target must be at least the same size as the source, just in case there's no 
optimization to be done.
* The current usernames and passwords must be available in order to create them after the copy.
* There must a sufficient window of time to do the copy as it can be quite long in relationship to the size of the database.


## Procedure

### Step 1.
Download the store-util distribution for the particular Neo4j version. The version of Neo4j is prefixed The 4.4.x releases handle Neo4j 4.4 and 4.3, 
respectively. There will be a 3.5.x release that will handle all Neo4j 3.5.x releases. **_Note_** though during the copy the Neo4j storage will be upgraded to the 
version that `store-utils` was built with.

### Step 2.
Extract the distribution to a local directory.

### Step 3.

Dump the index and constraint definitions to a file. Neo4j must be running.

    $ ./bin/dump

This command will authenticate to Neo4j and write all the index definitions to dump.json.  Each CLI command comes with help by convention of -h or --help.

Included with the dump command is the ability to change the index provider based on the attribute name. Given the type of data in the Brinqa database its better to have certain attributes use the Lucene index rather than BTree. Below is a common example but if there’s other low cardinally attributes they should be added here.

    $ ./bin/dump -l __dataModel__ 

NOTE: This command will use the local environment variables to authenticate to Neo4j. 

    NEO4J_URL
    NEO4J_USERNAME
    NEO4J_PASSWORD


### Step 4.
Run the optimization process such that it rebuilds the database in another directory.

    $ ./bin/storeCopy /data/neo4j /data/neo4j-optimized

This process can take several hours to process proportional to the size of the database and the percentage of fragmented space.

### Step 5.

Replace the old source directory with the new target directory applied above. Then start the database and monitor for issues. Optional run 

    $ neo4j-admin consistency-check 

to see if the database is proper.

### Step 6.

Recreate the users see https://neo4j.com/docs/cypher-manual/current/access-control/manage-users/

### Step 7.

Rebuilding the indexes, the tool can use the dump.json file created in the Step 3. to recreate the indexes.

    $ ./bin/load -f dump.json

This process can take a few hours to complete based on the size of the data.

NOTE: This command will use the local environment variables to authenticate to Neo4j.
