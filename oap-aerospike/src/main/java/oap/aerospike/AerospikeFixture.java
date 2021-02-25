package oap.aerospike;

import lombok.SneakyThrows;
import oap.testng.Env;
import oap.testng.Fixture;
import oap.util.Dates;

import java.io.IOException;

import static oap.testng.Fixture.Scope.*;

/**
 * Created by igor.petrenko on 2019-12-06.
 */
public class AerospikeFixture implements Fixture {
    public static final String TEST_NAMESPACE = Env.getEnvOrDefault("AEROSPIKE_TEST_NAMESPACE", "test");
    public static final String HOST = Env.getEnvOrDefault("AEROSPIKE_HOST", "localhost");
    public static final int PORT = Integer.parseInt(Env.getEnvOrDefault("AEROSPIKE_PORT", "3000"));
    public final int maxConnsPerNode;
    public final int connPoolsPerNode;
    private final Scope scope;
    public AerospikeClient aerospikeClient;

    public AerospikeFixture() {
        this(METHOD);
    }

    public AerospikeFixture(int maxConnsPerNode, int connPoolsPerNode) {
        this(METHOD, maxConnsPerNode, connPoolsPerNode);
    }

    public AerospikeFixture(Scope scope) {
        this(scope, 300, 1);
    }

    public AerospikeFixture(Scope scope, int maxConnsPerNode, int connPoolsPerNode) {
        this.scope = scope;
        this.maxConnsPerNode = maxConnsPerNode;
        this.connPoolsPerNode = connPoolsPerNode;
    }

    @Override
    public void beforeMethod() {
        init(METHOD);
    }

    @Override
    public void beforeClass() {
        init(CLASS);
    }

    @Override
    public void beforeSuite() {
        init(SUITE);
    }

    private void init(Scope scope) {
        if (this.scope != scope) return;

        System.setProperty("AEROSPIKE_TEST_NAMESPACE", TEST_NAMESPACE);
        System.setProperty("AEROSPIKE_HOST", HOST);
        System.setProperty("AEROSPIKE_PORT", String.valueOf(PORT));

        aerospikeClient = new AerospikeClient(HOST, PORT, true);
        aerospikeClient.maxConnsPerNode = maxConnsPerNode;
        aerospikeClient.connPoolsPerNode = connPoolsPerNode;
        aerospikeClient.connectionTimeout = Dates.s(120);
        aerospikeClient.start();
        aerospikeClient.waitConnectionEstablished();
        asDeleteAll();
    }


    public AerospikeFixture withScope(Scope scope) {
        return new AerospikeFixture(scope, maxConnsPerNode, connPoolsPerNode);
    }

    @SneakyThrows
    @Override
    public void afterMethod() {
        shutdown(METHOD);
    }

    @SneakyThrows
    @Override
    public void afterClass() {
        shutdown(CLASS);
    }

    @SneakyThrows
    @Override
    public void afterSuite() {
        shutdown(SUITE);
    }

    private void shutdown(Scope scope) throws IOException {
        if (this.scope != scope) return;

        asDeleteAll();
        aerospikeClient.close();
    }

    public void asDeleteAll() {
        var ret = aerospikeClient.getSets(TEST_NAMESPACE);
        ret.ifSuccess(sets -> {
            for (var set : sets) {
                aerospikeClient.deleteAll(TEST_NAMESPACE, set, 2);
            }
        });
    }
}
