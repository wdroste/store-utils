package org.neo4j.tool;

import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

/** Empty always */
public class DevNullLongLongMap extends LongLongHashMap {

    @Override
    public void put(long key, long value) {}

    @Override
    public void putPair(LongLongPair keyValuePair) {}

    @Override
    public void putAll(LongLongMap map) {}
}
