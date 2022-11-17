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

import groovy.lang.GroovyClassLoader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;

public class PredicateBuilder implements AutoCloseable {

    private final GroovyClassLoader gcl = new GroovyClassLoader();

    private static final String[] TEMPLATE = {
        "class Accept implements java.util.function.Predicate { ",
        "  boolean test(Object node) {",
        "    return %s",
        "  }",
        "}"
    };

    public Predicate<NodeObject> newInstance(String script) {
        if (StringUtils.isBlank(script)) {
            return nodeObject -> true;
        }
        final var scriptFmt = String.join("\n", TEMPLATE);
        final var clazzText = String.format(scriptFmt, script);
        final var clazz = gcl.parseClass(clazzText);
        try {
            //noinspection unchecked
            return (Predicate<NodeObject>) clazz.getConstructor().newInstance();
        } catch (NoSuchMethodException
                | InvocationTargetException
                | InstantiationException
                | IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.gcl.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
