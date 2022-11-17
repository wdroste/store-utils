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
