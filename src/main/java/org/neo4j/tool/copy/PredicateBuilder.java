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
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
