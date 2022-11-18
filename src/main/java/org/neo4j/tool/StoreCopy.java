/*
 * Copyright 2002 Brinqa, Inc. All rights reserved.
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
 */
package org.neo4j.tool;

import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_buffered_flush_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_direct_io;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;
import static org.neo4j.tool.util.Neo4jHelper.newBatchInserter;
import static org.neo4j.tool.util.Neo4jHelper.shutdown;
import static org.neo4j.tool.util.Print.println;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.configuration.Config;
import org.neo4j.tool.copy.NodeCopyJob;
import org.neo4j.tool.copy.RelationshipCopyJob;
import org.neo4j.tool.util.Neo4jHelper;
import org.neo4j.tool.util.Neo4jHelper.HighestInfo;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Need to validate the data as store copy is running. */
@Command(
        name = "copy",
        version = "copy 1.0",
        description =
                "Copies the source database to the target database, while optimizing size and consistency")
public class StoreCopy implements Runnable {

    // assumption different data directories
    @Parameters(index = "0", description = "Source directory for the data files.")
    private File sourceDataDirectory;

    // assumption different data directories
    @Parameters(index = "1", description = "Target directory for data files.")
    private File targetDataDirectory;

    @Option(
            names = {"-db", "--databaseName"},
            description = "Name of the database.",
            defaultValue = "neo4j")
    private String databaseName = "neo4j";

    @Option(
            names = {"-cfg", "--neo4jConf"},
            description = "Source 'neo4j.conf' file location.")
    private File sourceConfigurationFile;

    @Option(
            names = {"-s", "--scriptFile"},
            description = "Groovy script file that provides acceptance criteria for a node to be copied.")
    private File script;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new StoreCopy()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        try (final var job = new StoreCopyJob()) {
            job.run();
        }
    }

    class StoreCopyJob implements Runnable, Closeable {

        private final HighestInfo highestInfo;
        private final BatchInserter sourceDb;
        private final BatchInserter targetDb;
        private final String acceptanceScript;

        public StoreCopyJob() {
            // check source directory
            if (!sourceDataDirectory.isDirectory()) {
                throw new IllegalArgumentException(
                        "Source data directory does not exist: " + sourceDataDirectory);
            }
            // load configuration from files
            if (!targetDataDirectory.isDirectory() && !targetDataDirectory.mkdirs()) {
                throw new IllegalArgumentException(
                        "Unable to create directory for target database: " + targetDataDirectory);
            }

            // create a source configuration from main install
            final var sourceCfgBld = Config.newBuilder();
            if (null != sourceConfigurationFile && sourceConfigurationFile.isFile()) {
                sourceCfgBld.fromFile(sourceConfigurationFile.toPath());
            } else {
                sourceCfgBld.set(pagecache_memory, "4G");
                sourceCfgBld.set(allow_upgrade, false);
                sourceCfgBld.set(pagecache_direct_io, true);
                sourceCfgBld.set(pagecache_buffered_flush_enabled, true);
                sourceCfgBld.set(read_only_databases, Set.of(databaseName));
            }
            sourceCfgBld.set(data_directory, sourceDataDirectory.toPath());
            final var sourceConfig = sourceCfgBld.build();

            // change the target directory for the data
            final var targetConfig =
                    Config.newBuilder()
                            .fromConfig(sourceConfig)
                            .set(read_only_databases, Set.of())
                            .set(writable_databases, Set.of(databaseName))
                            .set(data_directory, targetDataDirectory.toPath())
                            .build();

            final var srcPath = sourceConfig.get(data_directory);

            println("Copying from %s to %s", srcPath, targetDataDirectory);

            // avoid nasty warning
            org.neo4j.internal.unsafe.IllegalAccessLoggerSuppressor.suppress();

            // find the highest node
            this.highestInfo =
                    Neo4jHelper.determineHighestNodeId(
                            sourceConfig, sourceDataDirectory, databaseName);

            // create inserters
            this.sourceDb = newBatchInserter(sourceConfig);
            this.targetDb = newBatchInserter(targetConfig);

            try {
                this.acceptanceScript = script != null ? Files.readString(script.toPath()) : null;
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public void run() {
            // copy nodes from source to target
            final var nodeCopyJob =
                    new NodeCopyJob(highestInfo.getNodeId(), sourceDb, targetDb, acceptanceScript);
            final LongLongMap copiedNodeIds = nodeCopyJob.process();

            // copy relationships from source to target
            final var relationshipCopyJob =
                    new RelationshipCopyJob(highestInfo.getRelationshipId(), sourceDb, targetDb);
            relationshipCopyJob.process(copiedNodeIds);
        }

        @Override
        public void close() {
            // shutdown the batch inserter
            shutdown(targetDb, "target");
            shutdown(sourceDb, "source");
        }
    }
}
