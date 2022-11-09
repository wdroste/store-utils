package org.neo4j.tool;

import static org.neo4j.tool.Print.println;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Rebuilds all the indexes one by one.
 *
 * <p>Neo4j will crash from either out of memory or too many files etc if you restore to many
 * indexes at once.
 */
@Command(
        name = "rebuildIndex",
        version = "rebuildIndex 1.0",
        description = "Rebuilds all the indexes one by one.")
public class RebuildIndex extends AbstractIndexCommand {

    @Option(
            defaultValue = "lastIndex",
            names = {"-r", "--resume"},
            description = "File to use for storing the last index.")
    protected File file;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new RebuildIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final Driver driver) throws IOException {
        final List<IndexData> indexes = readIndexes(driver);
        final var ver = VersionQuery.determineVersion(driver);
        String lastIndexName = file.isFile() ? Files.readString(file.toPath()) : null;
        for (final IndexData index : indexes) {

            // skip until resume index
            if (StringUtils.isNotBlank(lastIndexName)) {
                // skip until its found
                if (!lastIndexName.equals(index.getName())) {
                    continue;
                }
                // we found it, so set this to null
                lastIndexName = null;
                println("Resuming from index: %s", index.getName());
            }

            // save resume file
            Files.writeString(file.toPath(), index.getName());
            dropIndex(driver, index);
            createIndexWaitForCompletion(driver, ver, index);
        }
        println("Last index saved to %s", this.file);
    }

    @Override
    String getFilename() {
        return this.file.getName();
    }
}
