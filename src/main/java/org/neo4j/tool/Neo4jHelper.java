package org.neo4j.tool;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.internal.recordstorage.RecordIdType.NODE;
import static org.neo4j.internal.recordstorage.RecordIdType.RELATIONSHIP;
import static org.neo4j.tool.Print.println;

@Slf4j
public class Neo4jHelper {

    private static final Label[] NO_LABELS = new Label[0];

    @Value(staticConstructor = "of")
    public static class HighestInfo {

        long nodeId;
        long relationshipId;
    }

    static BatchInserter newBatchInserter(Config config) {
        try {
            return BatchInserters.inserter(DatabaseLayout.of(config));
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static HighestInfo determineHighestNodeId(Config sourceConfig, File sourceDataDirectory, String databaseName) {
        final var home = sourceConfig.get(GraphDatabaseSettings.neo4j_home);
        println("Neo4j Home: %s", home);

        final var managementServiceBld = new DatabaseManagementServiceBuilder(home);
        managementServiceBld.setConfig(data_directory, sourceDataDirectory.toPath());
        println("Source Data Directory: %s", sourceDataDirectory);

        final var managementService = managementServiceBld.build();
        final var graphDb = managementService.database(databaseName);

        final var api = (GraphDatabaseAPI) graphDb;
        final var idGenerators =
            api.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
        long highestNodeId = idGenerators.get(NODE).getHighestPossibleIdInUse();
        long highestRelId = idGenerators.get(RELATIONSHIP).getHighestPossibleIdInUse();
        managementService.shutdown();

        return HighestInfo.of(highestNodeId, highestRelId);
    }

    static Label[] filterLabels(BatchInserter db, Set<String> ignoreLabels, long node) {
        Collection<Label> labels = Iterables.asCollection(db.getNodeLabels(node));
        if (labels.isEmpty()) {
            return NO_LABELS;
        }
        if (!ignoreLabels.isEmpty()) {
            labels.removeIf(label -> ignoreLabels.contains(label.name()));
        }
        return labels.toArray(new Label[0]);
    }

    static int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    static void shutdown(BatchInserter inserter, String name) {
        try {
            println("Stopping '%s' database", name);
            inserter.shutdown();
        }
        catch (Exception e) {
            log.error("Error while stopping '" + name + "' database.", e);
        }
        println("Stopped '%s' database", name);
    }
}
