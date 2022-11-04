NOTES:

Store-Util is a utility that Will has forked from an existing Neo4j project that enables copying of a Neo4j database.  As part of this copy, it only grabs valid nodes to include in the copy.

The Utility is built against Neo4j 4.4, so if running against an earlier version, you’ll need to upgrade.

Once you have the Store-Util tar from Will, and have expanded it into a directory on the target machine, navigate to the root directory where you unpacked Store-Util and you’ll need to do the following steps:

Dump the indexes


./bin/dump --filename neo4j_indexes.dump
This runs a show indexes against the neo4j database and dumps all the indexes to a the file neo4j_indexes.dump.  You’ll need this to recreate the indexes after you copy the database since the utility does not copy over indexes.

Stop Neo4j

Neo4j needs to be stopped prior to running the copy.


systemctl stop neo4j
Run the Copy Utility


./bin/copy /data/neo4j /data/copy
This is the step that copies over the data.  It will take some time to run.  In the Neo4j directory, it will copy the contents of the neo4j database into a new folder (in this case copy).

Rename neo4j to Backup


mv /data/neo4j /data/backups
Rename copy to neo4j


mv /data/copy /data/neo4j
Uncomment auth_enabled=false

In the /etc/neo4j/neo4j.conf file, you’ll need to uncomment the line dbms.security.auth_enabled=false.  The database doesn’t have users yet, so you’ll need this to allow you to connect to the database.

Start Neo4j database

At this point, you’ll need to restart Neo4j


systemctl start neo4j
Create Users

You’ll need to create the two users, one identified in the /opt/brinqa/conf/brinqa.yml file in the neo4j section, generally username starting with b, and root user identifies in the environment variables NEO4J_USERNAME and NEO4J_PASSWORD.

Once you have these users, open a cypher-shell and execute the following for each user:


CREATE USER [username] SET PASSWORD '[password]' SET PASSWORD CHANGE NOT REQUIRED SET HOME DATABASE neo4j;
Load Indexes

As the last step, you can run the store-util load script to recreate the indexes.


./bin/load -f neo4j_indexes.dump
This process currently hangs on constraints against labels with no rows.  Until this gets corrected, you should separate out the constraints from the indexes, and run them separately.  

