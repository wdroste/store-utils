package org.neo4j.tool;

import org.neo4j.driver.v1.Driver;
import org.neo4j.tool.dto.IndexData;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.List;

import static org.neo4j.tool.util.Print.println;

/**
 * Dumps all the current indexes to a file for loading indexes.
 *
 * <p>Neo4j will crash from either out of memory or too many files etc. if you restore to many
 * indexes at once.
 */
@Command(
        name = "dropIndex",
        version = "dropIndex 1.0",
        description = "Drops indexes based on the dump file.")
public class DropIndex extends AbstractIndexCommand {

    @Option(
            required = true,
            names = {"-f", "--file"},
            description = "File to use to dump indexes.")
    protected File file;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new DropIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final Driver driver) {
        // query for all the indexes
        final List<IndexData> indexes = readIndexesFromFile(this.file);
        println("Dropping indexes from file: %s", this.file.getAbsoluteFile());
        for (IndexData index : indexes) {
            println("Dropping %s", index.getName());
            dropIndex(driver, index);
        }
    }

    @Override
    File getFile() {
        return this.file;
    }
}
