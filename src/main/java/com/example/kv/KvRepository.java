package com.example.kv;

import java.util.function.Consumer;

public interface KvRepository extends AutoCloseable {
    void put(String key, byte[] value);

    LookupResult get(String key);

    boolean delete(String key);

    void range(String keySince, String keyTo, Consumer<KvEntry> consumer);

    long count();

    @Override
    void close() throws Exception;

    record LookupResult(boolean found, byte[] value) {
    }

    record KvEntry(String key, byte[] value) {
    }
}
