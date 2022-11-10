package org.neo4j.tool;

import java.io.Closeable;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.configuration.Config;
import org.neo4j.tool.copy.NodeCopyJob;
import org.neo4j.tool.copy.RelationshipCopyJob;
import org.neo4j.tool.util.Neo4jHelper;
import org.neo4j.tool.util.Neo4jHelper.HighestInfo;

import org.eclipse.collections.api.map.primitive.LongLongMap;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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

/**
 * Need to validate the data as store copy is running.
 */
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
        required = true,
        names = {"-f", "--filename"},
        description = "Name of the file to load all the indexes.",
        defaultValue = "index_dump.json")
    protected String filename;

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
        names = {"-d", "--deleteWithLabel"},
        description = "Nodes to delete with the specified label.")
    private Set<String> deleteNodesByLabel = new HashSet<>();

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
            }
            else {
                sourceCfgBld.set(pagecache_memory, "4G");
                sourceCfgBld.set(allow_upgrade, false);
                sourceCfgBld.set(pagecache_direct_io, true);
                sourceCfgBld.set(pagecache_buffered_flush_enabled, true);
                sourceCfgBld.set(read_only_databases, Set.of(databaseName));
            }
            sourceCfgBld.set(data_directory, sourceDataDirectory.toPath());
            final var sourceConfig = sourceCfgBld.build();

            // change the target directory for the data
            Config targetConfig =
                Config.newBuilder()
                    .fromConfig(sourceConfig)
                    .set(read_only_databases, Set.of())
                    .set(writable_databases, Set.of(databaseName))
                    .set(data_directory, targetDataDirectory.toPath())
                    .build();

            final var srcPath = sourceConfig.get(data_directory);

            println("Copying from %s to %s", srcPath, targetDataDirectory);
            if (!deleteNodesByLabel.isEmpty()) {
                println("Delete nodes with label(s): %s", deleteNodesByLabel);
            }

            // avoid nasty warning
            org.neo4j.internal.unsafe.IllegalAccessLoggerSuppressor.suppress();

            // find the highest node
            this.highestInfo =
                Neo4jHelper.determineHighestNodeId(
                    sourceConfig, sourceDataDirectory, databaseName);

            // create inserters
            this.sourceDb = newBatchInserter(sourceConfig);
            this.targetDb = newBatchInserter(targetConfig);
        }

        @Override
        public void run() {
            // copy nodes from source to target
            final var nodeCopyJob =
                new NodeCopyJob(
                    highestInfo.getNodeId(), sourceDb, targetDb, deleteNodesByLabel);
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
