/*
 * Copyright 2024 Brinqa, Inc. All rights reserved.
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
package com.brinqa.tool.neo4j;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.Set;

/**
 * Need to validate the data as store copy is running.
 */
@Command(name = "dump", version = "1.0", description = "Dumps the contents of the database to a RockDB database.")
public class DatabaseDumpCLI implements Runnable {

    @Option(
            names = {"-n", "--no_auth"},
            description = "No authentication.")
    protected boolean noAuth;

    @Option(
            names = {"-a", "--url"},
            description = "Neo4j URL",
            defaultValue = "${NEO4J_URL:-bolt://localhost:7687}")
    protected String uri;

    @Option(
            names = {"-u", "--username"},
            description = "Neo4j Username",
            defaultValue = "${NEO4J_USERNAME}")
    protected String username;

    @Option(
            names = {"-p", "--password"},
            description = "Neo4j Password",
            defaultValue = "${NEO4J_PASSWORD}")
    protected String password;

    // assumption different data directories
    @Option(names = {"-d", "--directory"}, description = "Directory for data files.", defaultValue = "data")
    private File dataDirectory;

    @Option(names = {"-db", "--databaseName"}, description = "Name of the database.", defaultValue = "neo4j")
    private String databaseName = "neo4j";

    @Option(names = {"-l", "--deleteNodesWithLabel"}, description = "Delete nodes with label.")
    private Set<String> deleteNodesWithLabel;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new DatabaseDumpCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {

    }

}
