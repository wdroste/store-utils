package com.brinqa.tool.neo4j.job;

import lombok.Builder;
import lombok.Value;

import java.io.File;

@Value
@Builder
public class DatabaseDumpRequest {
    File dataDirectory;
}
