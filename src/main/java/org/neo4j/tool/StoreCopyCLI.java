package org.neo4j.tool;

import com.google.common.collect.ImmutableSet;
import com.google.gson.GsonBuilder;
import lombok.SneakyThrows;
import lombok.val;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tool.dto.StoreCopyConfiguration;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.MissingParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.UnmatchedArgumentException;

import java.io.File;
import java.nio.charset.StandardCharsets;

@Command(
    name = "storeCopy",
    version = "storeCopy 3.5",
    description = "Create a compacted copy of the offline database.")
public class StoreCopyCLI implements Runnable {

    @Parameters(index = "0", description = "Source Neo4j data directory.")
    protected File source;
    @Parameters(index = "1", description = "Target Neo4j data directory.")
    protected File target;
    @Parameters(index = "2", description = "Configuration file in JSON format.")
    protected File configuration;

    public static void main(String[] args) throws Exception {
        val commandLine = new CommandLine(new StoreCopyCLI());
        try {
            commandLine.parseArgs(args);
            int exitCode = commandLine.execute(args);
            System.exit(exitCode);
        }
        catch (MissingParameterException | UnmatchedArgumentException ex) {
            usage(commandLine);
            System.err.println(ex.getMessage());
        }
    }

    static void usage(CommandLine commandLine) {
        commandLine.usage(System.out);

        System.out.println("\nConfiguration Help: ");
        System.out.println("  deleteRelationshipsWithType - delete relationships between nodes with the specified types.");
        System.out.println("\nSource Directory is the directory where the data files reside not the `data` directory.");

        val cfg = new StoreCopyConfiguration();
        cfg.setDeleteNodesWithLabels(ImmutableSet.of("DeleteWithLabel1"));
        System.out.println("\nSample Configuration: ");
        val gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(cfg));
    }

    @Override
    @SneakyThrows
    public void run() {
        if (target.exists()) {
            throw new IllegalArgumentException("Target Directory already exists " + target);
        }
        if (!source.exists()) {
            throw new IllegalArgumentException("Source Database does not exist " + source);
        }
        if (!configuration.isFile()) {
            throw new IllegalArgumentException("Configuration file does not exist " + configuration);
        }
        // make sure there's an existing 'store'
        if (!(new File(source, "neostore").isFile())) {
            throw new IllegalArgumentException("Invalid path '" + source + "' must contain at least 'neostore' file.");
        }

        val gson = new GsonBuilder().setPrettyPrinting().create();
        val json = FileUtils.readTextFile(configuration, StandardCharsets.UTF_8);
        val cfg = gson.fromJson(json, StoreCopyConfiguration.class);

        val cpy = new StoreCopy(source, target, cfg);
        System.out.printf("Copying from %s to %s, with configuration: \n%s", source, target, configuration);
        System.out.println(gson.toJson(cfg));

        // start the copy
        cpy.run();
    }
}
