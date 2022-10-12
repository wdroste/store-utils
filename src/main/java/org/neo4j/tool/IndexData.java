package org.neo4j.tool;

import java.util.List;
import java.util.Objects;

public class IndexData {

    private final long id;
    private final String name;

    private final String state;
    private final float populationPercent;
    private final boolean uniqueness;
    private final String type;
    private final String entityType;
    private final List<String> labelsOrTypes;
    private final List<String> properties;
    private final String indexProvider;

    public IndexData(
            long id,
            String name,
            String state,
            float populationPercent,
            boolean uniqueness,
            String type,
            String entityType,
            List<String> labelsOrTypes,
            List<String> properties,
            String indexProvider) {
        this.id = id;
        this.name = name;
        this.state = state;
        this.populationPercent = populationPercent;
        this.uniqueness = uniqueness;
        this.type = type;
        this.entityType = entityType;
        this.labelsOrTypes = labelsOrTypes;
        this.properties = properties;
        this.indexProvider = indexProvider;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    public float getPopulationPercent() {
        return populationPercent;
    }

    public boolean isUniqueness() {
        return uniqueness;
    }

    public String getType() {
        return type;
    }

    public String getEntityType() {
        return entityType;
    }

    public List<String> getLabelsOrTypes() {
        return labelsOrTypes;
    }

    public List<String> getProperties() {
        return properties;
    }

    public String getIndexProvider() {
        return indexProvider;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexData)) {
            return false;
        }
        IndexData indexData = (IndexData) o;
        return id == indexData.id
                && Float.compare(indexData.populationPercent, populationPercent) == 0
                && uniqueness == indexData.uniqueness
                && name.equals(indexData.name)
                && state.equals(indexData.state)
                && type.equals(indexData.type)
                && entityType.equals(indexData.entityType)
                && labelsOrTypes.equals(indexData.labelsOrTypes)
                && properties.equals(indexData.properties)
                && indexProvider.equals(indexData.indexProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                name,
                state,
                populationPercent,
                uniqueness,
                type,
                entityType,
                labelsOrTypes,
                properties,
                indexProvider);
    }

    @Override
    public String toString() {
        return "IndexData{"
                + "id="
                + id
                + ", name='"
                + name
                + '\''
                + ", state='"
                + state
                + '\''
                + ", populationPercent="
                + populationPercent
                + ", uniqueness="
                + uniqueness
                + ", type='"
                + type
                + '\''
                + ", entityType='"
                + entityType
                + '\''
                + ", labelsOrTypes="
                + labelsOrTypes
                + ", properties="
                + properties
                + ", indexProvider='"
                + indexProvider
                + '\''
                + '}';
    }
}
