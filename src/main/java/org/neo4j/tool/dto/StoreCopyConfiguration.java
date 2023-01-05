package org.neo4j.tool.dto;

import com.google.gson.annotations.SerializedName;
import java.util.Set;
import lombok.Data;
import lombok.Singular;

@Data
public class StoreCopyConfiguration {

    String source;
    String target;

    @Singular
    @SerializedName("deleteNodesWithLabel")
    Set<String> deleteNodesWithLabels;
}
