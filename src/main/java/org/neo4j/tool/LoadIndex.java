package org.neo4j.tool;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.v1.Driver;
import org.neo4j.tool.VersionQuery.Neo4jVersion;
import org.neo4j.tool.dto.IndexData;

import lombok.val;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.tool.util.Print.println;

/**
 * Takes a dump file and creates each of the constraints and indexes from that file in a controlled
 * manner. In particular it waits until an index is online before moving to the next as adding to
 * many indexes at any onetime will result in either an OOME or a corrupted index that will need to
 * be refreshed again.
 */
@Command(
    name = "loadIndex",
    version = "loadIndex 1.0",
    description = "Creates indexes and constraints based on the file provided.")
public class LoadIndex extends AbstractIndexCommand {

    @Option(
        required = true,
        names = {"-f", "--filename"},
        description = "Name of the file to load all the indexes.",
        defaultValue = "dump.json")
    protected String filename;

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
    void execute(final Driver driver) {
        val ver = VersionQuery.determineVersion(driver);
        val indexNames = recreate ? Collections.<String>emptySet() : readIndexNames(driver);
        val fileIndexes = readIndexesFromFilename();
        final int total = fileIndexes.size();
        val count = new AtomicInteger();
        fileIndexes.stream()
            .peek(ignore -> count.incrementAndGet())
            .filter(indexData -> !indexData.getLabelsOrTypes().isEmpty())
            .filter(indexData -> !indexNames.contains(indexData.getName()))
            .forEach(
                indexData -> {
                    println("Progress: %d/%d", count.get(), total);
                    build(driver, indexData);
                });
    }

    Set<String> readIndexNames(final Driver driver) {
        return unmodifiableSet(readIndexes(driver).stream().map(IndexData::getName).collect(toSet()));
    }

    void build(final Driver driver, final IndexData index) {
        if (recreate) {
            dropIndex(driver, index);
        }
        createIndexWaitForCompletion(driver, index);
    }

    @Override
    String getFilename() {
        return this.filename;
    }
}
