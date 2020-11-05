package oap.aerospike;

import lombok.SneakyThrows;
import oap.testng.Env;
import oap.testng.Fixture;
import oap.util.Dates;

import java.io.IOException;

/**
 * Created by igor.petrenko on 2019-12-06.
 */
public class AerospikeFixture implements Fixture {
    public static final String TEST_NAMESPACE = Env.getEnvOrDefault("AEROSPIKE_TEST_NAMESPACE", "test");
    public static final String HOST = Env.getEnvOrDefault("AEROSPIKE_HOST", "localhost");
    public static final int PORT = Integer.parseInt(Env.getEnvOrDefault("AEROSPIKE_PORT", "3000"));
    public final int maxConnsPerNode;
    public final int connPoolsPerNode;
    private final boolean method;
    public AerospikeClient aerospikeClient;

    public AerospikeFixture() {
        this(true);
    }

    public AerospikeFixture(int maxConnsPerNode, int connPoolsPerNode) {
        this(true, maxConnsPerNode, connPoolsPerNode);
    }

    public AerospikeFixture(boolean method) {
        this(method, 300, 1);
    }

    public AerospikeFixture(boolean method, int maxConnsPerNode, int connPoolsPerNode) {
        this.method = method;
        this.maxConnsPerNode = maxConnsPerNode;
        this.connPoolsPerNode = connPoolsPerNode;
    }

    @Override
    public void beforeMethod() {
        if (method) init();
    }

    @Override
    public void beforeClass() {
        if (!method) init();
    }

    private void init() {
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

    @SneakyThrows
    @Override
    public void afterMethod() {
        if (method) shutdown();
    }

    @SneakyThrows
    @Override
    public void afterClass() {
        if (!method) shutdown();
    }

    private void shutdown() throws IOException {
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
