package org.neo4j.tool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.store.InvalidRecordException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import static org.neo4j.tool.Flusher.newFlusher;
import static org.neo4j.tool.Neo4jHelper.filterLabels;
import static org.neo4j.tool.Neo4jHelper.percent;
import static org.neo4j.tool.Print.printf;
import static org.neo4j.tool.Print.println;

@Slf4j
@AllArgsConstructor
public class NodeCopyJob {

    private final long highestNodeId;
    private final BatchInserter sourceDb;
    private final BatchInserter targetDb;
    private final Set<String> ignoreLabels;

    LongLongMap process() {
        final var copiedNodes = new LongLongHashMap(10_000_000).asSynchronized();

        long time = System.currentTimeMillis();
        final var notFound = new AtomicLong();
        final var removed = new AtomicLong();

        final Flusher flusher = newFlusher(sourceDb);

        long bound = highestNodeId + 1;
        for (long sourceNodeId = 0; sourceNodeId < bound; sourceNodeId++) {
            try {
                if (!sourceDb.nodeExists(sourceNodeId)) {
                    notFound.incrementAndGet();
                }
                else {
                    final var srcProps = sourceDb.getNodeProperties(sourceNodeId);
                    final var labels = filterLabels(sourceDb, ignoreLabels, sourceNodeId);
                    final var targetNodeId = targetDb.createNode(srcProps, labels);
                    copiedNodes.put(sourceNodeId, targetNodeId);
                }
            }
            catch (Exception e) {
                if (e instanceof InvalidRecordException
                    && e.getMessage().endsWith("not in use")) {
                    notFound.incrementAndGet();
                }
                else {
                    log.error(
                        "Failed to process, node ID: {} Message: {}",
                        sourceNodeId,
                        e.getMessage());
                }
            }
            // increment here because it's still needed above
            synchronized (StoreCopy.class) {
                long count = sourceNodeId + 1;
                if (count % 10_000 == 0) {
                    flusher.flush();
                    printf(".");
                }
                if (count % 500_000 == 0) {
                    int pct = percent(count, bound);
                    println(
                        " %d / %d (%d%%) unused %d removed %d",
                        count,
                        bound,
                        pct,
                        notFound.get(),
                        removed.get());
                }
            }
        }

        final var total = copiedNodes.size();
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        println(
            "%nCopying to highest sourceNodeId %d took %d seconds (%d rec/s). Unused Records %d (%d%%). Removed Records %d (%d%%). Total Copied: %d",
            total,
            time,
            total / time,
            notFound.get(),
            percent(notFound.get(), total),
            removed.get(),
            percent(removed.get(), total),
            copiedNodes.size());
        return copiedNodes;
    }

}
