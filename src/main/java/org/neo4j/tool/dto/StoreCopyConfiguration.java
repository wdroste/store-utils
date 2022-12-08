package org.neo4j.tool.dto;

import java.util.Set;

import com.google.gson.annotations.SerializedName;

import lombok.Data;
import lombok.Singular;

@Data
public class StoreCopyConfiguration {

    @Singular
    @SerializedName("relationshipTypesToIgnore")
    Set<String> ignoreRelTypes;
    @Singular
    @SerializedName("propertiesToIgnore")
    Set<String> ignoreProperties;
    @Singular
    @SerializedName("labelsToIgnore")
    Set<String> ignoreLabels;
    @Singular
    @SerializedName("labelsToDelete")
    Set<String> deleteNodesWithLabels;
}
