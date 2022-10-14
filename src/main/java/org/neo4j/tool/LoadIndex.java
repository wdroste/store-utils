package org.neo4j.tool;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.neo4j.tool.Print.println;

import java.util.List;
import java.util.Set;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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

    private static final Logger LOG = LoggerFactory.getLogger(LoadIndex.class);

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
        final List<IndexData> fileIndexes = readIndexesFromFilename();
        final Set<String> indexNames = recreate ? Set.of() : readIndexNames(driver);

        fileIndexes.stream()
                .filter(indexData -> !indexData.getLabelsOrTypes().isEmpty())
                .filter(indexData -> !indexNames.contains(indexData.getName()))
                .forEach(indexData -> build(driver, indexData));
    }

    Set<String> readIndexNames(final Driver driver) {
        return readIndexes(driver).stream().map(IndexData::getName).collect(toUnmodifiableSet());
    }

    void build(final Driver driver, final IndexData indexData) {
        if (recreate) {
            dropIndex(driver, indexData);
        }

        indexCreate(driver, indexData);

        // wait for completion
        int pct = 0;
        while (pct < 100) {
            progressPercentage(pct);
            pct = (int) indexProgress(driver, indexData.getName());
            progressPercentage(pct);
        }
    }

    void dropIndex(Driver driver, IndexData indexData) {
        // query for all the indexes
        final var query = dropQuery(indexData);
        println(query);
        try {
            writeTransaction(driver, query);
        } catch (Throwable th) {
            LOG.error("Failed to drop index: {}", query, th);
        }
    }

    String dropQuery(IndexData data) {
        final var FMT = data.isUniqueness() ? "DROP CONSTRAINT %s" : "DROP INDEX %s";
        return String.format(FMT, data.getName());
    }

    @Override
    String getFilename() {
        return this.filename;
    }
}
