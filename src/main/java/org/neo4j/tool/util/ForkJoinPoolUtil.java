package org.neo4j.tool.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForkJoinPoolUtil {
    private static final AtomicLong counter = new AtomicLong();

    public static ForkJoinPool newPool(final String prefix, int parallelism) {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory =
                pool -> {
                    final ForkJoinWorkerThread thread =
                            ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    thread.setName(prefix + "-" + counter.incrementAndGet());
                    return thread;
                };
        final Thread.UncaughtExceptionHandler handler = (t, e) -> log.error("Failed terribly", e);
        return new ForkJoinPool(parallelism, factory, handler, false);
    }
}
