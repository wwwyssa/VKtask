package com.example.kv;

import com.example.kv.proto.CountRequest;
import com.example.kv.proto.CountResponse;
import com.example.kv.proto.DeleteRequest;
import com.example.kv.proto.DeleteResponse;
import com.example.kv.proto.GetRequest;
import com.example.kv.proto.GetResponse;
import com.example.kv.proto.KvStoreGrpc;
import com.example.kv.proto.PutRequest;
import com.example.kv.proto.PutResponse;
import com.example.kv.proto.RangeItem;
import com.example.kv.proto.RangeRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public final class KvServiceImpl extends KvStoreGrpc.KvStoreImplBase {
    private final KvRepository repository;

    public KvServiceImpl(KvRepository repository) {
        this.repository = repository;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            byte[] value = request.hasValue() ? request.getValue().getValue().toByteArray() : null;
            repository.put(request.getKey(), value);
            responseObserver.onNext(PutResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            KvRepository.LookupResult result = repository.get(request.getKey());
            GetResponse.Builder builder = GetResponse.newBuilder().setFound(result.found());
            if (result.found() && result.value() != null) {
                builder.setValue(BytesValue.of(ByteString.copyFrom(result.value())));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            boolean deleted = repository.delete(request.getKey());
            responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeItem> responseObserver) {
        try {
            repository.range(request.getKeySince(), request.getKeyTo(), entry -> {
                RangeItem.Builder builder = RangeItem.newBuilder().setKey(entry.key());
                if (entry.value() != null) {
                    builder.setValue(BytesValue.of(ByteString.copyFrom(entry.value())));
                }
                responseObserver.onNext(builder.build());
            });
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = repository.count();
            responseObserver.onNext(CountResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }
}
