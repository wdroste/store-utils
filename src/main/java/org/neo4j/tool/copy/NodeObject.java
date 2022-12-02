/*
 * Copyright 2002 Brinqa, Inc. All rights reserved.
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
     * Use this to test certain criteria without throwing exceptions.
     *
     * @param name property name
     * @return true if there's a value else false.
     */
    public boolean containsProperty(String name) {
        return this.properties.containsKey(name);
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
