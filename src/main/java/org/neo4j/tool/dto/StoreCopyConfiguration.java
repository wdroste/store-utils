package org.neo4j.tool.dto;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.Singular;

import java.util.Set;

@Data
public class StoreCopyConfiguration {

    String source;
    String target;

    @Singular
    @SerializedName("deleteNodesWithLabel")
    Set<String> deleteNodesWithLabels;
}
