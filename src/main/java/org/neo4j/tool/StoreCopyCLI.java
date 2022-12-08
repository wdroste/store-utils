package org.neo4j.tool;

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.google.gson.GsonBuilder;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.tool.dto.StoreCopyConfiguration;

import lombok.SneakyThrows;
import lombok.val;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

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
        int exitCode = new CommandLine(new StoreCopyCLI()).execute(args);
        System.exit(exitCode);
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

        val gson = new GsonBuilder().setPrettyPrinting().create();
        val json = FileUtils.readTextFile(configuration, StandardCharsets.UTF_8);
        val cfg = gson.fromJson(json, StoreCopyConfiguration.class);

        val cpy = new StoreCopy(
            source,
            target,
            cfg.getIgnoreRelTypes(),
            cfg.getIgnoreProperties(),
            cfg.getIgnoreLabels(),
            cfg.getDeleteNodesWithLabels()
        );
        System.out.printf("Copying from %s to %s, with configuration: %s", source, target, configuration);
        System.out.println(gson.toJson(cfg));

        // start the copy
        cpy.run();
    }
}