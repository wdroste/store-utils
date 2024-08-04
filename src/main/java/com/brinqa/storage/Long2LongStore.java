package com.brinqa.storage;

import static org.rocksdb.CompressionType.ZSTD_COMPRESSION;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.rocksdb.CompressionOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Long2LongStore implements Closeable {

    public static final Logger LOG = LoggerFactory.getLogger(Long2LongStore.class);

    private final File directory;
    private final SerializationHandler<Long> keyHandler;
    private final SerializationHandler<Long> valueHandler;
    private final AtomicReference<RocksDB> db = new AtomicReference<>();
    private final AtomicInteger counter = new AtomicInteger(0);

    public Long2LongStore() {
        this.keyHandler = LongSerializationHandler.INSTANCE;
        this.valueHandler = LongSerializationHandler.INSTANCE;

        // insure the library is loaded safely
        synchronized (Long2LongStore.class) {
            RocksDB.loadLibrary();
        }

        // storage directory for the Long2Long map
        this.directory = newTempDirectory();

        // build out the database
        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setCompressionType(ZSTD_COMPRESSION);

            try (CompressionOptions compressionOptions = new CompressionOptions()) {
                compressionOptions.setEnabled(true);
                this.db.set(RocksDB.open(options, this.directory.getAbsolutePath()));
            }
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }
    }

    File newTempDirectory() {
        try {
            return Files.createTempDirectory("store-utils").toFile();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void put(final Long key, final Long value) {
        assert key != null && value != null;
        final byte[] rawKey = keyHandler.toBytes(key);
        final byte[] rawValue = valueHandler.toBytes(value);
        try {
            if (null == db.get().get(rawKey)) {
                counter.incrementAndGet();
            }
            db.get().put(rawKey, rawValue);
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }
    }

    public Long get(final Long key) {
        assert key != null;
        try {
            final byte[] rawKey = keyHandler.toBytes(key);
            final byte[] raw = this.db.get().get(rawKey);
            return (raw == null) ? null : this.valueHandler.fromBytes(raw);
        } catch (RocksDBException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void delete() {
        try {
            close();
        } catch (Exception e) {
            // na
        }
        // attempt to delete the directory
        try (Options options = new Options()) {
            RocksDB.destroyDB(this.directory.getAbsolutePath(), options);
        } catch (RocksDBException e) {
            LOG.error("Unable to delete database: {}", this.directory);
        }
    }

    public int size() {
        return this.counter.get();
    }

    @Override
    public void close() {
        final RocksDB theDb = this.db.get();
        if (null != theDb) {
            theDb.close();
            this.db.set(null);
        }
    }
}
