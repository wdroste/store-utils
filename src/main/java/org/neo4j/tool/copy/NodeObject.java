package org.neo4j.tool.copy;

import groovy.lang.GroovyObjectSupport;
import groovy.lang.MissingPropertyException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NodeObject extends GroovyObjectSupport {

    private final List<String> labels;
    private final Map<String, Object> properties;

    public NodeObject(List<String> labels, Map<String, Object> properties) {
        this.labels = Collections.unmodifiableList(labels);
        this.properties = properties;
    }

    /**
     * Get the property from the node, dynamically.
     *
     * @param propertyName name of the node property
     * @return value from the property or throw {@link MissingPropertyException}
     */
    @Override
    public Object getProperty(String propertyName) {
        if ("labels".equals(propertyName)) {
            return this.labels;
        }
        if (this.properties.containsKey(propertyName)) {
            return this.properties.get(propertyName);
        }
        throw new MissingPropertyException(propertyName, null);
    }
}
