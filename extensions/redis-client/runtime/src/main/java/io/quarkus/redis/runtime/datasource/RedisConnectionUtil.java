package io.quarkus.redis.runtime.datasource;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.RedisConnection;

public final class RedisConnectionUtil {

    private static final Cancellable SUBSCRIBING = () -> {
    };
    private static final Cancellable CANCELLED = () -> {
    };
    private static final Cancellable TERMINATED = () -> {
    };

    private RedisConnectionUtil() {
    }

    public static <T> Uni<T> withConnection(Redis redis, Function<RedisConnection, Uni<T>> function) {
        return Uni.createFrom().emitter(emitter -> {
            ConnectionExecution<T> execution = new ConnectionExecution<>(emitter, function);
            emitter.onTermination(execution::cancel);
            // Acquisition must continue so that a connection delivered after cancellation can be closed.
            redis.connect().subscribe().with(emitter.context(), execution::connected, execution::fail);
        });
    }

    private static final class ConnectionExecution<T> {

        private final UniEmitter<? super T> emitter;
        private final Function<RedisConnection, Uni<T>> function;
        private final AtomicReference<Cancellable> state = new AtomicReference<>();

        private ConnectionExecution(UniEmitter<? super T> emitter, Function<RedisConnection, Uni<T>> function) {
            this.emitter = emitter;
            this.function = function;
        }

        private void connected(RedisConnection connection) {
            if (!state.compareAndSet(null, SUBSCRIBING)) {
                connection.closeAndForget();
                return;
            }

            Uni<T> result;
            try {
                result = nonNull(function.apply(connection), "The function must not return null");
            } catch (Throwable failure) {
                result = Uni.createFrom().failure(failure);
            }
            Cancellable action = result.onTermination().call(connection::close)
                    .subscribe().with(emitter.context(), this::complete, this::fail);
            if (!state.compareAndSet(SUBSCRIBING, action) && state.get() == CANCELLED) {
                action.cancel();
            }
        }

        private void complete(T result) {
            if (terminate()) {
                emitter.complete(result);
            }
        }

        private void fail(Throwable failure) {
            if (terminate()) {
                emitter.fail(failure);
            }
        }

        private boolean terminate() {
            Cancellable previous = state.getAndSet(TERMINATED);
            return previous != CANCELLED && previous != TERMINATED;
        }

        private void cancel() {
            while (true) {
                Cancellable current = state.get();
                if (current == CANCELLED || current == TERMINATED) {
                    return;
                }
                if (state.compareAndSet(current, CANCELLED)) {
                    if (current != null && current != SUBSCRIBING) {
                        current.cancel();
                    }
                    return;
                }
            }
        }
    }
}
