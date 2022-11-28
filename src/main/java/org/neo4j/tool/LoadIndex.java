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
package org.neo4j.tool;

import static org.neo4j.tool.util.Print.println;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.tool.dto.IndexData;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Takes a dump file and creates each of the constraints and indexes from that file in a controlled
 * manner. In particular, it waits until an index is online before moving to the next as adding to
 * many indexes at any onetime will result in either an OOME or a corrupted index that will need to
 * be refreshed again.
 */
@Command(
        name = "loadIndex",
        version = "loadIndex 1.0",
        description =
                "Creates indexes and constraints based on the file provided, skips existing indexes and constraints by name.")
public class LoadIndex extends AbstractIndexCommand {

    @Option(
            required = true,
            names = {"-f", "--filename"},
            description = "File to load all the indexes.",
            defaultValue = "dump.json")
    protected File file;

    @Option(
            names = {"-r", "--recreate"},
            description = "Recreate each of the indexes in the file.")
    protected boolean recreate;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new LoadIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final IndexManager indexManager) {
        final var ver = indexManager.determineVersion();
        final var indexNames = indexManager.readIndexNames();
        final var fileIndexes = indexManager.readIndexesFromFile(file);
        final int total = fileIndexes.size();
        final var count = new AtomicInteger();
        for (IndexData indexData : fileIndexes) {
            count.incrementAndGet();
            println("Progress: %d/%d", count.get(), total);
            if (indexData.getLabelsOrTypes().isEmpty()) {
                println("Filtering as there's no Label: %s", indexData);
                continue;
            }
            // if recreate always drop and then create
            if (!recreate && indexNames.contains(indexData.getName())) {
                println("Index with name '%s' already exists, skipping.", indexData.getName());
                continue;
            }
            // create index
            indexManager.buildIndexOrConstraint(ver, indexData, recreate);
        }
    }
}
