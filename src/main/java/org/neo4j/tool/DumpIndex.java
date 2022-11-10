package org.neo4j.tool;

import static org.neo4j.tool.util.Print.println;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.Driver;
import org.neo4j.tool.dto.IndexData;
import org.neo4j.tool.dto.IndexDataComparator;
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
            names = {"-f", "--filename"},
            description = "Name of the file to dump.",
            defaultValue = "dump.json")
    protected String filename;

    @Option(
            names = {"-l", "--lucene"},
            description = "Replace any index containing a property name with a Lucene index")
    protected Set<String> lucene;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new DumpIndex()).execute(args);
        System.exit(exitCode);
    }

    @Override
    void execute(final Driver driver) {
        // query for all the indexes
        final List<IndexData> indexes = readIndexes(driver);
        println("Building index file: %s", this.filename);
        final List<IndexData> writeIndexes =
                (lucene == null || lucene.isEmpty()) ? indexes : luceneIndex(indexes);
        final List<IndexData> sortedIndexes =
                writeIndexes.stream()
                        .sorted(new IndexDataComparator())
                        .collect(Collectors.toList());
        writeIndexes(sortedIndexes);
    }

    /** Substitute the index for lucene */
    private List<IndexData> luceneIndex(List<IndexData> indexes) {
        return indexes.stream().map(this::checkLucene).collect(Collectors.toList());
    }

    IndexData checkLucene(IndexData index) {
        boolean l = index.getProperties().stream().anyMatch(p -> lucene.contains(p));
        return l ? modifyIndexProvider(index) : index;
    }

    IndexData modifyIndexProvider(IndexData data) {
        return data.toBuilder().indexProvider("lucene+native-3.0").build();
    }

    @Override
    String getFilename() {
        return this.filename;
    }
}
