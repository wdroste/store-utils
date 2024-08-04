package com.brinqa.storage;

public interface SerializationHandler<T> {

    /**
     * @param value object to convert to a byte array.
     * @return converted object in byte array form.
     */
    byte[] toBytes(T value);

    /**
     * De-serialize
     *
     * @param data byte array used to convert back to original object.
     * @return object from the bytes provided.
     */
    T fromBytes(byte[] data);
}
