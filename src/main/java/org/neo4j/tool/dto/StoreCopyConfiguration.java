package org.neo4j.tool.dto;

import java.util.Set;

import com.google.gson.annotations.SerializedName;

import lombok.Data;
import lombok.Singular;

@Data
public class StoreCopyConfiguration {

    @Singular
    @SerializedName("deleteRelationshipsWithType")
    Set<String> ignoreRelTypes;
    @Singular
    @SerializedName("filterPropertiesFromNode")
    Set<String> ignoreProperties;
    @Singular
    @SerializedName("filterLabelsFromNode")
    Set<String> ignoreLabels;
    @Singular
    @SerializedName("deleteNodesWithLabel")
    Set<String> deleteNodesWithLabels;
}
