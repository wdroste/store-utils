package org.neo4j.tool;

import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.batchinsert.internal.BatchRelationship;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "copy",
        version = "copy 1.0",
        description =
                "Copies the source database to the target database, while optimizing size and consistency")
public class StoreCopy implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StoreCopy.class);

    private static final Label[] NO_LABELS = new Label[0];

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
            names = {"-irt", "--ignoreRelationshipTypes"},
            description = "Relationship types to ignore.")
    private Set<String> ignoreRelationshipTypes = new HashSet<>();

    @Option(
            names = {"-ip", "--ignoreProperties"},
            description = "Properties to ignore.")
    private Set<String> ignoreProperties = new HashSet<>();

    @Option(
            names = {"-il", "--ignoreLabels"},
            description = "Labels to ignore.")
    private Set<String> ignoreLabels = new HashSet<>();

    @Option(
            names = {"-dl", "--deleteLabels"},
            description = "Nodes with labels to delete (exclude).")
    private Set<String> deleteLabels = new HashSet<>();

    @Option(
            names = {"--keep-node-ids"},
            description = "Maintain the IDs for the nodes.",
            defaultValue = "true")
    private boolean keepNodeIds = true;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new StoreCopy()).execute(args);
        System.exit(exitCode);
    }

    static void println(String fmt, Object... args) {
        System.out.printf(fmt + "%n", args);
    }

    static void printf(String fmt, Object... args) {
        System.out.printf(fmt + "%n", args);
    }

    @Override
    public void run() {
        new CopyStoreJob().run();
    }

    class CopyStoreJob implements Runnable {

        private final Config sourceConfig;
        private final Config targetConfig;

        public CopyStoreJob() {
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
            if (null != sourceConfigurationFile && !sourceConfigurationFile.isFile()) {
                sourceCfgBld.fromFile(sourceConfigurationFile.toPath());
            }
            sourceCfgBld.set(data_directory, sourceDataDirectory.toPath());
            sourceConfig = sourceCfgBld.build();

            // change the target directory for the data
            this.targetConfig =
                    Config.newBuilder()
                            .fromConfig(this.sourceConfig)
                            .set(data_directory, targetDataDirectory.toPath())
                            .build();

            final var srcPath = this.sourceConfig.get(data_directory);

            println("Copying from %s to %s", srcPath, targetDataDirectory);
            if (!ignoreRelationshipTypes.isEmpty()) {
                println("Ignore relationship types: %s", ignoreRelationshipTypes);
            }
            if (!ignoreProperties.isEmpty()) {
                println("Ignore properties: %s", ignoreRelationshipTypes);
            }
            if (!ignoreLabels.isEmpty()) {
                println("Ignore label(s): %s", ignoreRelationshipTypes);
            }
            if (!deleteLabels.isEmpty()) {
                println("Delete nodes with label(s): %s", ignoreRelationshipTypes);
            }
            println("Keep node IDs: %s", keepNodeIds);
        }

        @Override
        public void run() {
            final HighestInfo highestInfo = getHighestNodeId();
            // build source database management

            final var sourceDb = newBatchInserter(sourceConfig);
            final var targetDb = newBatchInserter(targetConfig);

            final var copiedNodeIds = copyNodes(sourceDb, targetDb, highestInfo.nodeId);

            copyRelationships(sourceDb, targetDb, copiedNodeIds, highestInfo.relationshipId);
            println("Stopping target database");
            targetDb.shutdown();
            println("Stopped target database");

            try {
                println("Stopping source database");
                sourceDb.shutdown();
            } catch (Exception e) {
                log.error("Error while stopping source database.", e);
            }
            println("Stopped source database");
        }

        BatchInserter newBatchInserter(Config config) {
            try {
                return BatchInserters.inserter(DatabaseLayout.of(config));
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        class HighestInfo {

            final long nodeId;
            final long relationshipId;

            HighestInfo(long nodeId, long relationshipId) {
                this.nodeId = nodeId;
                this.relationshipId = relationshipId;
            }
        }

        private HighestInfo getHighestNodeId() {
            final var home = sourceConfig.get(GraphDatabaseSettings.neo4j_home);
            final var managementServiceBld = new DatabaseManagementServiceBuilder(home);
            managementServiceBld.setConfig(data_directory, sourceDataDirectory.toPath());
            final var managementService = managementServiceBld.build();
            GraphDatabaseService graphDb = managementService.database(databaseName);

            GraphDatabaseAPI api = (GraphDatabaseAPI) graphDb;
            final var idGenerators =
                    api.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
            long highestNodeId = idGenerators.get(RecordIdType.NODE).getHighestPossibleIdInUse();
            long highestRelId =
                    idGenerators.get(RecordIdType.RELATIONSHIP).getHighestPossibleIdInUse();
            managementService.shutdown();

            return new HighestInfo(highestNodeId, highestRelId);
        }

        private LongLongMap copyNodes(
                BatchInserter sourceDb, BatchInserter targetDb, long highestNodeId) {
            MutableLongLongMap copiedNodes =
                    keepNodeIds ? new LongLongHashMap(10_000_000) : new DevNullLongLongMap();

            long time = System.currentTimeMillis();
            long notFound = 0;
            long removed = 0;
            long node = -1;
            while (node <= highestNodeId) {
                node++;
                try {
                    if (!sourceDb.nodeExists(node)) {
                        notFound++;
                        continue;
                    }

                    if (labelInSet(sourceDb.getNodeLabels(node), deleteLabels)) {
                        removed++;
                        continue;
                    }

                    // found
                    long newNodeId =
                            targetDb.createNode(
                                    getProperties(sourceDb.getNodeProperties(node)),
                                    labelsArray(sourceDb, node));
                    copiedNodes.put(node, newNodeId);
                } catch (Exception e) {
                    if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException
                            && e.getMessage().endsWith("not in use")) {
                        notFound++;
                    }
                }
                if (node % 10000 == 0) {
                    System.out.print(".");
                }
                if (node % 500000 == 0) {
                    System.out.printf(
                            " %d / %d (%d%%) unused %d removed %d%n",
                            node, highestNodeId, percent(node, highestNodeId), notFound, removed);
                }
            }
            time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
            System.out.printf(
                    "%n copying of %d node records took %d seconds (%d rec/s). Unused Records %d (%d%%). Removed Records %d (%d%%).%n",
                    node,
                    time,
                    node / time,
                    notFound,
                    percent(notFound, node),
                    removed,
                    percent(removed, node));
            return copiedNodes;
        }

        boolean createRelationship(
                BatchInserter targetDb,
                BatchInserter sourceDb,
                BatchRelationship rel,
                LongLongMap copiedNodeIds) {
            long startNodeId = rel.getStartNode(), endNodeId = rel.getEndNode();
            if (copiedNodeIds != null) {
                startNodeId = copiedNodeIds.get(startNodeId);
                endNodeId = copiedNodeIds.get(endNodeId);
            }
            if (startNodeId == -1L || endNodeId == -1L) {
                return false;
            }
            final RelationshipType type = rel.getType();
            try {
                final var props = getProperties(sourceDb.getRelationshipProperties(rel.getId()));
                targetDb.createRelationship(startNodeId, endNodeId, type, props);
                return true;
            } catch (Exception e) {
                log.error("Failed to create relationship.", e);
            }
            return false;
        }

        void copyRelationships(
                BatchInserter sourceDb,
                BatchInserter targetDb,
                LongLongMap copiedNodeIds,
                long highestRelId) {
            long time = System.currentTimeMillis();
            long relId = 0;
            long notFound = 0;
            long removed = 0;
            while (relId <= highestRelId) {
                try {
                    final var rel = sourceDb.getRelationshipById(relId++);
                    final var type = rel.getType().name();
                    if (!ignoreRelationshipTypes.contains(type)) {
                        if (!createRelationship(targetDb, sourceDb, rel, copiedNodeIds)) {
                            removed++;
                        }
                    } else {
                        removed++;
                    }
                } catch (Exception e) {
                    if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException
                            && e.getMessage().endsWith("not in use")) {
                        notFound++;
                    }
                }
                if (relId % 10000 == 0) {
                    System.out.print(".");
                }
                if (relId % 500000 == 0) {
                    printf(
                            " %d / %d (%d%%) unused %d removed %d%n",
                            relId, highestRelId, percent(relId, highestRelId), notFound, removed);
                }
            }
            time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
            final var msg =
                    "%n copying of %d relationship records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n";
            printf(
                    msg,
                    relId,
                    time,
                    relId / time,
                    notFound,
                    percent(notFound, relId),
                    removed,
                    percent(removed, relId));
        }
    }

    private static boolean labelInSet(Iterable<Label> nodeLabels, Set<String> labelSet) {
        if (labelSet == null || labelSet.isEmpty()) {
            return false;
        }
        for (final Label nodeLabel : nodeLabels) {
            if (labelSet.contains(nodeLabel.name())) {
                return true;
            }
        }
        return false;
    }

    private Label[] labelsArray(BatchInserter db, long node) {
        Collection<Label> labels = Iterables.asCollection(db.getNodeLabels(node));
        if (labels.isEmpty()) {
            return NO_LABELS;
        }
        if (!this.ignoreLabels.isEmpty()) {
            labels.removeIf(label -> this.ignoreLabels.contains(label.name()));
        }
        return labels.toArray(new Label[0]);
    }

    private Map<String, Object> getProperties(Map<String, Object> pc) {
        if (pc.isEmpty()) {
            return Collections.emptyMap();
        }
        if (!this.ignoreProperties.isEmpty()) {
            pc.keySet().removeAll(this.ignoreProperties);
        }
        return pc;
    }

    private int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }
}
