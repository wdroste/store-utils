package com.brinqa.storage;

/** Simple serialization for long. */
public class LongSerializationHandler implements SerializationHandler<Long> {

    public static final LongSerializationHandler INSTANCE = new LongSerializationHandler();

    @Override
    public byte[] toBytes(Long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    @Override
    public Long fromBytes(byte[] data) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (data[i] & 0xFF);
        }
        return result;
    }
}
