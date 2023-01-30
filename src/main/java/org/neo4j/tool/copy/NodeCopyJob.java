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

import static java.lang.System.currentTimeMillis;
import static org.neo4j.tool.util.Flusher.newFlusher;
import static org.neo4j.tool.util.Print.println;
import static org.neo4j.tool.util.Print.progressPercentage;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.tool.util.Flusher;

/** NOTE: {@link BatchInserter} is not thread safe. */
@Slf4j
@AllArgsConstructor
public class NodeCopyJob {

    private final long highestNodeId;
    private final BatchInserter sourceDb;
    private final BatchInserter targetDb;
    private final String acceptanceScript;

    public Long2LongMap process() {
        try (PredicateBuilder builder = new PredicateBuilder()) {
            final var predicate = builder.newInstance(acceptanceScript);
            return new NodeCopyProcessor(predicate).process();
        } catch (Exception ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    @RequiredArgsConstructor
    class NodeCopyProcessor {

        // acceptance criteria script
        private final Predicate<NodeObject> acceptance;

        // stats
        private final long bound = highestNodeId + 1;
        private final long start = currentTimeMillis();

        private final Flusher flusher = newFlusher(sourceDb);

        private final Long2LongMap copiedNodes = new Long2LongOpenHashMap();

        private long count = 0L;
        private long notFound = 0L;
        private long removed = 0L;
        private long progress = System.currentTimeMillis();

        public Long2LongMap process() {
            // run the task
            LongStream.range(0, bound).forEach(this::processNode);
            // print the final percentage
            progressPercentage(count, bound);
            // print the final stats
            printFinalStats(copiedNodes.size());
            return copiedNodes;
        }

        void processNode(long sourceNodeId) {
            try {
                if (!sourceDb.nodeExists(sourceNodeId)) {
                    notFound++;
                } else {
                    final long targetNodeId = copyNode(sourceNodeId);
                    if (targetNodeId > 0) {
                        copiedNodes.put(sourceNodeId, targetNodeId);
                    }
                }
            } catch (Exception e) {
                handleFailure(e, sourceNodeId);
            }
            // flush content for memory usage
            if (++count % 20_000 == 0) {
                flusher.flush();
            }
            // check if it's been a second since last checked
            long now = System.currentTimeMillis();
            if ((now - progress) > 1000) {
                progress = now;
                progressPercentage(count, bound);
            }
        }

        long copyNode(long sourceNodeId) {
            // read previous node data, since it exists
            final var properties = sourceDb.getNodeProperties(sourceNodeId);
            final var labels = Iterables.asList(sourceDb.getNodeLabels(sourceNodeId));

            // create a node object for criteria testing
            final var labelNames = labels.stream().map(Label::name).collect(Collectors.toList());
            final var node = new NodeObject(labelNames, properties);
            if (acceptance.test(node)) {
                // accepted create the node
                final var nodeLabels = labels.toArray(new Label[] {});
                return targetDb.createNode(properties, nodeLabels);
            }
            // failed acceptance criteria filter
            removed++;
            return -1L;
        }

        private void handleFailure(Exception e, long sourceNodeId) {
            if (e instanceof InvalidRecordException && e.getMessage().endsWith("not in use")) {
                notFound++;
            } else {
                final var FMT = "Failed to process, node ID: {} Message: {}";
                log.error(FMT, sourceNodeId, e.getMessage());
                removed++;
            }
        }

        private void printFinalStats(long total) {
            final var time = Math.max(1, (currentTimeMillis() - start) / 1000);
            final var fmt =
                    new String[] {
                        "%nCopying to highest source node %d took %d seconds (%d rec/s).",
                        "Unused Records: %d",
                        "Removed Records: %d",
                        "Total Copied: %d"
                    };
            println(
                    String.join("%n", fmt),
                    highestNodeId,
                    time,
                    highestNodeId / time,
                    notFound,
                    removed,
                    total);
        }
    }
}
