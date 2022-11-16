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
package org.neo4j.tool.util;

import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.internal.recordstorage.RecordIdType.NODE;
import static org.neo4j.internal.recordstorage.RecordIdType.RELATIONSHIP;
import static org.neo4j.tool.util.Print.println;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class Neo4jHelper {

    private static final Label[] NO_LABELS = new Label[0];

    @Value(staticConstructor = "of")
    public static class HighestInfo {

        long nodeId;
        long relationshipId;
    }

    public static BatchInserter newBatchInserter(Config config) {
        try {
            return BatchInserters.inserter(DatabaseLayout.of(config));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static HighestInfo determineHighestNodeId(
            Config sourceConfig, File sourceDataDirectory, String databaseName) {
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

    public static Label[] filterLabels(BatchInserter db, Set<String> ignoreLabels, long node) {
        List<Label> labels = Iterables.asList(db.getNodeLabels(node));
        if (labels.isEmpty()) {
            return NO_LABELS;
        }
        if (!ignoreLabels.isEmpty()) {
            labels.removeIf(label -> ignoreLabels.contains(label.name()));
        }
        return labels.toArray(new Label[0]);
    }

    public static int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    public static void shutdown(BatchInserter inserter, String name) {
        try {
            println("Stopping '%s' database", name);
            inserter.shutdown();
        } catch (Exception e) {
            log.error("Error while stopping '" + name + "' database.", e);
        }
        println("Stopped '%s' database", name);
    }
}
