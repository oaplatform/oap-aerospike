package oap.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.EventLoop;
import oap.reflect.Reflect;
import oap.util.Throwables;

import java.util.function.BiFunction;

/**
 * Created by igor.petrenko on 2020-09-16.
 */
@FunctionalInterface
public interface AerospikeClientFunction<T> {
    T apply(com.aerospike.client.AerospikeClient t, EventLoop eventLoop) throws AerospikeException;

    default BiFunction<AerospikeClient, EventLoop, T> asFunction() {
        return (t, eventLoop) -> {
            try {
                return this.apply(t, eventLoop);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        };
    }

    default BiFunction<AerospikeClient, EventLoop, T> orElseThrow(Class<? extends RuntimeException> clazz) {
        return (t, eventLoop) -> {
            try {
                return this.apply(t, eventLoop);
            } catch (Exception e) {
                throw Reflect.newInstance(clazz, e);
            }
        };
    }
}
