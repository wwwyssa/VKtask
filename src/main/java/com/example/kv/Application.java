package com.example.kv;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class Application {
    private Application() {
    }

    public static void main(String[] args) throws Exception {
        String tarantoolHost = env("TARANTOOL_HOST", "127.0.0.1");
        int tarantoolPort = Integer.parseInt(env("TARANTOOL_PORT", "3301"));
        String tarantoolUser = env("TARANTOOL_USER", "guest");
        String tarantoolPassword = env("TARANTOOL_PASSWORD", "");
        String tarantoolSpace = env("TARANTOOL_SPACE", "KV");
        long connectTimeoutMs = Long.parseLong(env("TARANTOOL_CONNECT_TIMEOUT_MS", "5000"));
        long requestTimeoutMs = Long.parseLong(env("TARANTOOL_REQUEST_TIMEOUT_MS", "10000"));
        int grpcPort = Integer.parseInt(env("GRPC_PORT", "9090"));

        TarantoolKvRepository repository = new TarantoolKvRepository(
                tarantoolHost,
                tarantoolPort,
                tarantoolUser,
                tarantoolPassword,
                tarantoolSpace,
                connectTimeoutMs,
                requestTimeoutMs
        );

        Server server = ServerBuilder.forPort(grpcPort)
                .addService(new KvServiceImpl(repository))
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(server, repository)));

        System.out.printf("gRPC server started on port %d%n", grpcPort);
        server.awaitTermination();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static void shutdown(Server server, TarantoolKvRepository repository) {
        try {
            server.shutdown();
            server.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        try {
            repository.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close Tarantool client", e);
        }
    }
}
