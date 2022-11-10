package org.neo4j.tool.util;

import java.lang.reflect.Field;
import java.util.Map;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.internal.BatchInserterImpl;
import org.neo4j.batchinsert.internal.FileSystemClosingBatchInserter;
import org.neo4j.internal.recordstorage.DirectRecordAccess;
import org.neo4j.internal.recordstorage.DirectRecordAccessSet;

public interface Flusher {
    void flush();

    static Flusher newFlusher(BatchInserter db) {
        try {
            final Field delegate =
                    FileSystemClosingBatchInserter.class.getDeclaredField("delegate");
            delegate.setAccessible(true);
            db = (BatchInserter) delegate.get(db);
            final Field field = BatchInserterImpl.class.getDeclaredField("recordAccess");
            field.setAccessible(true);

            final DirectRecordAccessSet recordAccessSet = (DirectRecordAccessSet) field.get(db);
            final Field cacheField = DirectRecordAccess.class.getDeclaredField("batch");
            cacheField.setAccessible(true);

            return () -> {
                try {
                    ((Map<?, ?>) cacheField.get(recordAccessSet.getNodeRecords())).clear();
                    ((Map<?, ?>) cacheField.get(recordAccessSet.getRelRecords())).clear();
                    ((Map<?, ?>) cacheField.get(recordAccessSet.getPropertyRecords())).clear();
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("Error clearing cache " + cacheField, e);
                }
            };
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException("Error accessing cache field ", e);
        }
    }
}
