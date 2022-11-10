package org.neo4j.tool.copy;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.tool.util.Flusher.newFlusher;
import static org.neo4j.tool.util.Neo4jHelper.filterLabels;
import static org.neo4j.tool.util.Neo4jHelper.percent;
import static org.neo4j.tool.util.Print.printf;
import static org.neo4j.tool.util.Print.println;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.tool.util.Flusher;

@Slf4j
@AllArgsConstructor
public class NodeCopyJob {

    private final long highestNodeId;
    private final BatchInserter sourceDb;
    private final BatchInserter targetDb;
    private final Set<String> deleteNodesWithLabels;

    public LongLongMap process() {
        return new NodeCopyProcessor().process();
    }

    class NodeCopyProcessor {

        // stats
        private final long bound = highestNodeId + 1;
        private final long start = currentTimeMillis();
        private final AtomicLong notFound = new AtomicLong();
        private final AtomicLong removed = new AtomicLong();
        private final Flusher flusher = newFlusher(sourceDb);
        private final LongLongHashMap copiedNodes = new LongLongHashMap(10_000_000);

        public LongLongMap process() {

            for (long sourceNodeId = 0; sourceNodeId < bound; sourceNodeId++) {
                try {
                    if (!sourceDb.nodeExists(sourceNodeId)) {
                        notFound.incrementAndGet();
                    } else {
                        final var srcProps = sourceDb.getNodeProperties(sourceNodeId);
                        final var labels =
                                filterLabels(sourceDb, deleteNodesWithLabels, sourceNodeId);
                        final var targetNodeId = targetDb.createNode(srcProps, labels);
                        copiedNodes.put(sourceNodeId, targetNodeId);
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
            }
            printFinalStats();

            return copiedNodes;
        }

        private void handleFailure(Exception e, long sourceNodeId) {
            if (e instanceof InvalidRecordException && e.getMessage().endsWith("not in use")) {
                notFound.incrementAndGet();
            } else {
                log.error(
                        "Failed to process, node ID: {} Message: {}", sourceNodeId, e.getMessage());
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

        private void printFinalStats() {
            final var total = copiedNodes.size();
            final var time = Math.max(1, (currentTimeMillis() - start) / 1000);
            println(
                    "%nCopying to highest source node %d took %d seconds (%d rec/s). Unused Records %d (%d%%). Removed Records %d (%d%%). Total Copied: %d",
                    total,
                    time,
                    total / time,
                    notFound.get(),
                    percent(notFound.get(), total),
                    removed.get(),
                    percent(removed.get(), total),
                    copiedNodes.size());
        }
    }
}
