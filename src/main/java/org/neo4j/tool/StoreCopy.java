package org.neo4j.tool;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.batchinsert.internal.BatchInserterImpl;
import org.neo4j.batchinsert.internal.BatchRelationship;
import org.neo4j.batchinsert.internal.FileSystemClosingBatchInserter;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.DirectRecordAccess;
import org.neo4j.internal.recordstorage.DirectRecordAccessSet;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@Command(name = "copy", mixinStandardHelpOptions = true, version = "copy 1.0",
    description = "Copies the source database to the target database, while optimizing size and consistency")
public class StoreCopy implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(StoreCopy.class);

    private static final Label[] NO_LABELS = new Label[];

    // assumption different data directories
    @Parameters(index = "0", description = "Source 'neo4j.conf' file to use.")
    private File sourceConfFile;

    // assumption different data directories
    @Parameters(index = "1", description = "Target 'neo4j.conf' file to use.")
    private File targetConfFile;

    @Option(names = {"-db", "--databaseName"}, description = "Name of the database.", defaultValue = "neo4j")
    private String databaseName = "neo4j";

    @Option(names = {"-irt", "--ignoreRelationshipTypes"}, description = "Relationship types to ignore.")
    private Set<String> relationshipTypes2Ignore = new HashSet<>();

    @Option(names = {"-ip", "--ignoreProperties"}, description = "Properties to ignore.")
    private Set<String> properties2Ignore = new HashSet<>();

    @Option(names = {"-il", "--ignoreLabels"}, description = "Labels to ignore.")
    private Set<String> labels2Ignore = new HashSet<>();

    @Option(names = {"-dl", "--deleteLabels"}, description = "Labels to delete.")
    private Set<String> labels2Delete = new HashSet<>();

    @Option(names = {"--keep-node-ids"}, description = "Maintain the IDs for the nodes.", defaultValue = "true")
    private boolean keepNodeIds = true;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new StoreCopy()).execute(args);
        System.exit(exitCode);
    }

    private Config sourceConfig;
    private Config targetConfig;


    @Override
    public Integer call() {
        // load configuration from files
        this.sourceConfig = Config.newBuilder().fromFile(sourceConfFile.toPath()).build();
        this.targetConfig = Config.newBuilder().fromFile(targetConfFile.toPath()).build();


        System.out.printf(
            "Copying from %s to %s ignoring rel-types %s ignoring properties %s ignoring labels %s removing nodes with labels %s keep node ids %s %n",
            sourceDir,
            targetDir,
            ignoreRelTypes,
            ignoreProperties,
            ignoreLabels,
            deleteNodesWithLabels,
            keepNodeIds);
        copyStore();
        return 0;
    }

    interface Flusher {

        void flush();
    }

    void copyStore() {
        if (target.exists()) {
            throw new IllegalArgumentException("Target Directory already exists " + target);
        }
        if (!source.exists()) {
            throw new IllegalArgumentException("Source Database does not exist " + source);
        }

        Pair<Long, Long> highestIds = getHighestNodeId(source);
        String pageCacheSize = System.getProperty("dbms.pagecache.memory", "2G");

        // build source database management
        final var managementService =
            new DatabaseManagementServiceBuilder(sourceDir)
                .loadPropertiesFromFile(pathToConfig + "neo4j.conf")
                .build();
        GraphDatabaseService graphDb = managementService.database(DEFAULT_DATABASE_NAME);

        // build source database management

        Map<String, String> targetConfig =
            MapUtil.stringMap("dbms.pagecache.memory", pageCacheSize);
        BatchInserter targetDb = BatchInserters.inserter(target, targetConfig);

        Map<String, String> sourceConfig =
            MapUtil.stringMap(
                "dbms.pagecache.memory",
                System.getProperty("dbms.pagecache.memory.source", pageCacheSize),
                "dbms.read_only",
                "true");
        BatchInserter sourceDb = BatchInserters.inserter(source, sourceConfig);
        Flusher flusher = getFlusher(sourceDb);

        logs = new

            PrintWriter(new FileWriter(new File(target, "store-copy.log")));

        final var copiedNodeIds =
            copyNodes(
                sourceDb,
                targetDb,
                ignoreProperties,
                ignoreLabels,
                deleteNodesWithLabels,
                highestIds.first(),
                flusher,
                stableNodeIds);

        copyRelationships(
            sourceDb,
            targetDb,
            ignoreRelTypes,
            ignoreProperties,
            copiedNodeIds,
            highestIds.other(),

            flusher);
        System.out.println("Stopping target database");
        targetDb.shutdown();
        System.out.println("Stopped target database");
        try {
            System.out.println("Stopping source database");
            sourceDb.shutdown();
        }
        catch (
            Exception e) {
            logs.append(
                String.format(
                    "Noncritical error closing the source database:%n%s",
                    Exceptions.stringify(e)));
        }
        System.out.println("Stopped source database");
        logs.close();
    }

    private static Flusher getFlusher(BatchInserter db) {
        try {
            Field delegate = FileSystemClosingBatchInserter.class.getDeclaredField("delegate");
            delegate.setAccessible(true);
            db = (BatchInserter) delegate.get(db);
            Field field = BatchInserterImpl.class.getDeclaredField("recordAccess");
            field.setAccessible(true);
            final DirectRecordAccessSet recordAccessSet = (DirectRecordAccessSet) field.get(db);
            final Field cacheField = DirectRecordAccess.class.getDeclaredField("batch");
            cacheField.setAccessible(true);
            return () -> {
                try {
                    ((Map<?, ?>) cacheField.get(recordAccessSet.getNodeRecords())).clear();
                    ((Map<?, ?>) cacheField.get(recordAccessSet.getRelRecords())).clear();
                    ((Map<?, ?>) cacheField.get(recordAccessSet.getPropertyRecords())).clear();
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException("Error clearing cache " + cacheField, e);
                }
            };
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Error accessing cache field ", e);
        }
    }

    private static Pair<Long, Long> getHighestNodeId(File source) {
        GraphDatabaseAPI api = (GraphDatabaseAPI) factory().newEmbeddedDatabase(source);
        IdGeneratorFactory idGenerators =
            api.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
        long highestNodeId = idGenerators.get(RecordIdType.NODE).getHighestPossibleIdInUse();
        long highestRelId = idGenerators.get(RecordIdType.RELATIONSHIP).getHighestPossibleIdInUse();
        api.shutdown();
        return Pair.of(highestNodeId, highestRelId);
    }

    private static void copyRelationships(
        final BatchInserter sourceDb,
        final BatchInserter targetDb,
        final Set<String> ignoreRelTypes,
        final Set<String> ignoreProperties,
        final LongLongMap copiedNodeIds,
        final long highestRelId,
        final Flusher flusher) {
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        long removed = 0;
        while (relId <= highestRelId) {
            BatchRelationship rel = null;
            String type = null;
            try {
                rel = sourceDb.getRelationshipById(relId++);
                type = rel.getType().name();
                if (!ignoreRelTypes.contains(type)) {
                    if (!createRelationship(
                        targetDb, sourceDb, rel, ignoreProperties, copiedNodeIds)) {
                        removed++;
                    }
                }
                else {
                    removed++;
                }
            }
            catch (Exception e) {
                if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException
                    && e.getMessage().endsWith("not in use")) {
                    notFound++;
                }
                else {
                    addLog(
                        rel,
                        "copy Relationship: " + (relId - 1) + "-[:" + type + "]" + "->?",
                        e.getMessage());
                }
            }
            if (relId % 10000 == 0) {
                System.out.print(".");
                logs.flush();
            }
            if (relId % 500000 == 0) {
                flusher.flush();
                System.out.printf(
                    " %d / %d (%d%%) unused %d removed %d%n",
                    relId, highestRelId, percent(relId, highestRelId), notFound, removed);
            }
        }
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        System.out.printf(
            "%n copying of %d relationship records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n",
            relId,
            time,
            relId / time,
            notFound,
            percent(notFound, relId),
            removed,
            percent(removed, relId));
    }

    private static int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    private static long firstNode(BatchInserter sourceDb, long highestNodeId) {
        long node = -1;
        while (++node <= highestNodeId) {
            if (sourceDb.nodeExists(node) && !sourceDb.getNodeProperties(node).isEmpty()) {
                return node;
            }
        }
        return -1;
    }

    private static void flushCache(BatchInserter sourceDb, long node) {
        Map<String, Object> nodeProperties = sourceDb.getNodeProperties(node);
        Iterator<Map.Entry<String, Object>> iterator = nodeProperties.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<String, Object> firstProp = iterator.next();
            sourceDb.nodeHasProperty(node, firstProp.getKey());
            sourceDb.setNodeProperty(node, firstProp.getKey(), firstProp.getValue()); // force flush
            System.out.print(" flush");
        }
    }

    private boolean createRelationship(
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
            Map<String, Object> props =
                getProperties(sourceDb.getRelationshipProperties(rel.getId()));
            //            if (props.isEmpty()) props =
            // Collections.<String,Object>singletonMap("old_id",rel.getId()); else
            // props.put("old_id",rel.getId());
            targetDb.createRelationship(startNodeId, endNodeId, type, props);
            return true;
        }
        catch (Exception e) {
            addLog(
                rel,
                "create Relationship: " + startNodeId + "-[:" + type + "]" + "->" + endNodeId,
                e.getMessage());
            return false;
        }
    }

    private LongLongMap copyNodes(
        BatchInserter sourceDb,
        BatchInserter targetDb,
        Set<String> ignoreProperties,
        Set<String> ignoreLabels,
        Set<String> deleteNodesWithLabels,
        long highestNodeId,
        Flusher flusher,
        boolean stableNodeIds) {
        MutableLongLongMap copiedNodes = stableNodeIds ? null : new LongLongHashMap(10_000_000);
        long time = System.currentTimeMillis();
        long node = 0;
        long notFound = 0;
        long removed = 0;
        while (node <= highestNodeId) {
            try {
                if (sourceDb.nodeExists(node)) {
                    if (labelInSet(sourceDb.getNodeLabels(node), deleteNodesWithLabels)) {
                        removed++;
                    }
                    else {
                        if (stableNodeIds) {
                            targetDb.createNode(
                                node,
                                getProperties(sourceDb.getNodeProperties(node)),
                                labelsArray(sourceDb, node));
                        }
                        else {
                            long newNodeId =
                                targetDb.createNode(
                                    getProperties(
                                        sourceDb.getNodeProperties(node),
                                        ignoreProperties),
                                    labelsArray(sourceDb, node, ignoreLabels));
                            copiedNodes.put(node, newNodeId);
                        }
                    }
                }
                else {
                    notFound++;
                }
            }
            catch (Exception e) {
                if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException
                    && e.getMessage().endsWith("not in use")) {
                    notFound++;
                }
                else {
                    addLog(node, e.getMessage());
                }
            }
            node++;
            if (node % 10000 == 0) {
                System.out.print(".");
            }
            if (node % 500000 == 0) {
                flusher.flush();
                logs.flush();
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

    private static boolean labelInSet(Iterable<Label> nodeLabels, Set<String> labelSet) {
        if (labelSet == null || labelSet.isEmpty()) {
            return false;
        }
        for (Label nodeLabel : nodeLabels) {
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
        if (!this.labels2Ignore.isEmpty()) {
            labels.removeIf(label -> this.labels2Ignore.contains(label.name()));
        }
        return labels.toArray(new Label[0]);
    }

    private Map<String, Object> getProperties(Map<String, Object> pc) {
        if (pc.isEmpty()) {
            return Collections.emptyMap();
        }
        if (!this.properties2Ignore.isEmpty()) {
            pc.keySet().removeAll(this.properties2Ignore);
        }
        return pc;
    }

    private static void addLog(BatchRelationship rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private static void addLog(long node, String message) {
        logs.append(String.format("Node: %s %s%n", node, message));
    }
}
