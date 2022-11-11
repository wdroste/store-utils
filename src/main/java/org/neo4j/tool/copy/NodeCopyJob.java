package org.neo4j.tool.copy;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.tool.util.Flusher.newFlusher;
import static org.neo4j.tool.util.Neo4jHelper.percent;
import static org.neo4j.tool.util.Print.printf;
import static org.neo4j.tool.util.Print.println;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.tool.util.Flusher;

@Slf4j
@AllArgsConstructor
public class NodeCopyJob {

    private final long highestNodeId;
    private final BatchInserter sourceDb;
    private final BatchInserter targetDb;
    private final String acceptanceScript;

    public LongLongMap process() {
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
        private final AtomicLong notFound = new AtomicLong();
        private final AtomicLong removed = new AtomicLong();
        private final Flusher flusher = newFlusher(sourceDb);

        public LongLongMap process() {
            final var copiedNodes = new LongLongHashMap(10_000_000);
            final LongConsumer consumer =
                    sourceNodeId -> {
                        try {
                            if (!sourceDb.nodeExists(sourceNodeId)) {
                                notFound.incrementAndGet();
                            } else {
                                final long targetNodeId = copyNode(sourceNodeId);
                                if (targetNodeId > 0) {
                                    synchronized (this) {
                                        copiedNodes.put(sourceNodeId, targetNodeId);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            handleFailure(e, sourceNodeId);
                        }
                        // count is node index + 1 as its zero based
                        long count = sourceNodeId + 1;
                        printStats(count);

                        // flush content for memory usage
                        if (count % 20_000 == 0) {
                            flusher.flush();
                        }
                    };
            LongStream.range(0, bound).parallel().forEach(consumer);
            printFinalStats(copiedNodes.size());
            return copiedNodes;
        }

        long copyNode(long sourceNodeId) {
            final var properties = sourceDb.getNodeProperties(sourceNodeId);
            final var labels = Iterables.asList(sourceDb.getNodeLabels(sourceNodeId));

            final var labelNames = labels.stream().map(Label::name).collect(Collectors.toList());
            final var node = new NodeObject(labelNames, properties);
            if (acceptance.test(node)) {
                // accepted create the node
                final var nodeLabels = labels.toArray(new Label[] {});
                return targetDb.createNode(properties, nodeLabels);
            }
            // failed acceptance criteria filter
            removed.incrementAndGet();
            return -1L;
        }

        private void handleFailure(Exception e, long sourceNodeId) {
            if (e instanceof InvalidRecordException && e.getMessage().endsWith("not in use")) {
                notFound.incrementAndGet();
            } else {
                final var FMT = "Failed to process, node ID: {} Message: {}";
                log.error(FMT, sourceNodeId, e.getMessage());
                removed.incrementAndGet();
            }
        }

        private synchronized void printStats(long count) {
            if (count % 10_000 == 0) {
                printf(".");
            }
            if (count % 500_000 == 0) {
                int pct = percent(count, bound);
                println(
                        " %d / %d (%d%%) unused %d removed %d",
                        count, bound, pct, notFound.get(), removed.get());
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
                    notFound.get(),
                    removed.get(),
                    total);
        }
    }
}
