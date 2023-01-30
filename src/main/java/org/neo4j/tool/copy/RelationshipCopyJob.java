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
package org.neo4j.tool.copy;

import static org.neo4j.tool.util.Flusher.newFlusher;
import static org.neo4j.tool.util.Neo4jHelper.percent;
import static org.neo4j.tool.util.Print.printf;
import static org.neo4j.tool.util.Print.progressPercentage;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.internal.BatchRelationship;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.tool.util.Flusher;

@Slf4j
@AllArgsConstructor
public class RelationshipCopyJob {
    private final long highestRelationshipId;
    private final BatchInserter sourceDb;
    private final BatchInserter targetDb;

    public void process(Long2LongMap copiedNodeIds) {

        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        long removed = 0;

        long progress = System.currentTimeMillis();
        final Flusher flusher = newFlusher(sourceDb);
        while (relId <= highestRelationshipId) {
            try {
                final var rel = sourceDb.getRelationshipById(relId);
                if (!createRelationship(rel, copiedNodeIds)) {
                    removed++;
                }
            } catch (Exception e) {
                if (e instanceof InvalidRecordException && e.getMessage().endsWith("not in use")) {
                    notFound++;
                } else {
                    log.error(
                            "Failed to process, relationship ID: {} Message: {}",
                            relId,
                            e.getMessage());
                }
            }
            // increment here for counts, its still needed above
            long count = ++relId;
            if (count % 10000 == 0) {
                flusher.flush();
            }

            // check if it's been a second since last checked
            long now = System.currentTimeMillis();
            if ((now - progress) > 1000) {
                progress = now;
                progressPercentage(count, highestRelationshipId);
            }
        }
        progressPercentage(relId, highestRelationshipId);
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        final var msg =
                "%nCopying of %d relationship records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n";
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

    boolean createRelationship(BatchRelationship rel, Long2LongMap copiedNodeIds) {
        try {
            final long startNodeId = copiedNodeIds.getOrDefault(rel.getStartNode(), -1L);
            final long endNodeId = copiedNodeIds.getOrDefault(rel.getEndNode(), -1L);
            if (startNodeId == -1L || endNodeId == -1L) {
                return false;
            }
            final var props = sourceDb.getRelationshipProperties(rel.getId());
            targetDb.createRelationship(startNodeId, endNodeId, rel.getType(), props);
            return true;
        } catch (Exception e) {
            log.error("Failed to create relationship.", e);
        }
        return false;
    }
}
