package org.neo4j.tool;

import static org.neo4j.tool.Print.println;

import com.google.gson.GsonBuilder;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Dumps all the current indexes to a file for loading indexes.
 *
 * <p>Neo4j will crash from either out of memory or too many files etc if you restore to many
 * indexes at once.
 */
@Command(
        name = "dumpIndex",
        version = "dumpIndex 1.0",
        description = "Dumps all the indexes and constraints to a file for loadIndex.")
public class DumpIndex extends AbstractIndexCommand {

    @Option(
            required = false,
            names = {"-f", "--filename"},
            description = "Name of the file to dump.",
            defaultValue = "dump.json")
    protected String filename;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new DumpIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final Driver driver) {
        // query for all the indexes
        try (Session session = driver.session()) {
            assert session != null;
            final var indexes =
                    session.readTransaction(
                            tx -> {
                                final List<IndexData> indexData = new ArrayList<>();
                                final var result = tx.run("show indexes;");
                                result.forEachRemaining(
                                        record -> indexData.add(fromRecord(record)));
                                return indexData;
                            });
            println("Building index file: %s", this.filename);
            writeIndexes(indexes);
        }
    }

    void writeIndexes(List<IndexData> indexes) {
        final var gson = new GsonBuilder().create();
        try (final var wrt = new BufferedWriter(new FileWriter(this.filename))) {
            for (IndexData index : indexes) {
                wrt.write(gson.toJson(index));
                wrt.newLine();
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
