package com.example.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.TarantoolResponse;
import io.tarantool.mapping.Tuple;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class TarantoolKvRepository implements KvRepository {
    private static final int RANGE_BATCH_SIZE = 10_000;

    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace space;
    private final String spaceName;
    private final long requestTimeoutMs;

    public TarantoolKvRepository(String host,
                                 int port,
                                 String user,
                                 String password,
                                 String spaceName,
                                 long connectTimeoutMs,
                                 long requestTimeoutMs) throws Exception {
        var builder = TarantoolFactory.box()
                .withHost(host)
                .withPort(port)
                .withConnectTimeout(connectTimeoutMs);

        if (user != null && !user.isBlank()) {
            builder.withUser(user);
        }
        if (password != null && !password.isBlank()) {
            builder.withPassword(password);
        }

        this.client = builder.build();
        this.spaceName = Objects.requireNonNull(spaceName, "spaceName must not be null");
        this.space = client.space(spaceName);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    @Override
    public void put(String key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        await(space.replace(Arrays.asList(key, value)));
    }

    @Override
    public LookupResult get(String key) {
        Objects.requireNonNull(key, "key must not be null");

        SelectOptions options = SelectOptions.builder()
                .withIndex(0)
                .withLimit(1)
                .build();

        SelectResponse<List<Tuple<List<?>>>> response = await(space.select(List.of(key), options));
        List<Tuple<List<?>>> tuples = response.get();

        if (tuples == null || tuples.isEmpty()) {
            return new LookupResult(false, null);
        }

        List<?> row = tuples.get(0).get();
        byte[] value = row.size() > 1 ? toBytes(row.get(1)) : null;
        return new LookupResult(true, value);
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "key must not be null");
        Tuple<List<?>> deleted = await(space.delete(List.of(key)));
        return deleted != null && deleted.get() != null && !deleted.get().isEmpty();
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KvEntry> consumer) {
        Objects.requireNonNull(keySince, "keySince must not be null");
        Objects.requireNonNull(keyTo, "keyTo must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");

        if (keySince.compareTo(keyTo) > 0) {
            return;
        }

        byte[] position = null;
        while (true) {
            SelectOptions.Builder optionsBuilder = SelectOptions.builder()
                    .withIndex(0)
                    .withIterator(BoxIterator.GE)
                    .withLimit(RANGE_BATCH_SIZE)
                    .fetchPosition();

            if (position != null) {
                optionsBuilder.after(position);
            }

            SelectResponse<List<Tuple<List<?>>>> response = await(space.select(List.of(keySince), optionsBuilder.build()));
            List<Tuple<List<?>>> rows = response.get();
            if (rows == null || rows.isEmpty()) {
                return;
            }

            boolean stop = false;
            for (Tuple<List<?>> tuple : rows) {
                List<?> row = tuple.get();
                String currentKey = String.valueOf(row.get(0));
                if (currentKey.compareTo(keyTo) > 0) {
                    stop = true;
                    break;
                }
                byte[] value = row.size() > 1 ? toBytes(row.get(1)) : null;
                consumer.accept(new KvEntry(currentKey, value));
            }

            if (stop) {
                return;
            }

            position = response.getPosition();
            if (position == null) {
                return;
            }
        }
    }

    @Override
    public long count() {
        String script = "local sn = ...; local s = box.space[sn]; if s == nil then return 0 end; return s:count()";
        TarantoolResponse<List<?>> response = await(client.eval(script, List.of(spaceName)));
        List<?> result = response.get();
        if (result == null || result.isEmpty()) {
            return 0L;
        }

        Object value = result.get(0);
        if (value instanceof Number number) {
            return number.longValue();
        }

        throw new IllegalStateException("Unexpected count() response type: " + value);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    private <T> T await(CompletableFuture<T> future) {
        try {
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Tarantool request failed", e);
        }
    }

    private static byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer copy = byteBuffer.slice();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        if (value instanceof String string) {
            return string.getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Unsupported Tarantool value type: " + value.getClass().getName());
    }
}
