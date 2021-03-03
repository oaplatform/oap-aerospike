package oap.aerospike;

import lombok.SneakyThrows;
import oap.system.Env;
import oap.testng.EnvFixture;
import oap.util.Dates;

import java.io.IOException;

import static oap.testng.Fixture.Scope.*;

/**
 * Created by igor.petrenko on 2019-12-06.
 */
public class AerospikeFixture extends EnvFixture {
    public static final String TEST_NAMESPACE = Env.get("AEROSPIKE_TEST_NAMESPACE", "test");
    public static final String HOST = Env.get("AEROSPIKE_HOSTS", "localhost");
    public static final int PORT = Integer.parseInt(Env.get("AEROSPIKE_PORT", "3000"));
    public final int maxConnsPerNode;
    public final int connPoolsPerNode;
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

        define("AEROSPIKE_TEST_NAMESPACE", TEST_NAMESPACE);
        define("AEROSPIKE_HOSTS", HOST);
        define("AEROSPIKE_PORT", PORT);
    }

    @Override
    public void beforeMethod() {
        super.beforeMethod();

        init(METHOD);
    }

    @Override
    public void beforeClass() {
        super.beforeClass();

        init(CLASS);
    }

    @Override
    public void beforeSuite() {
        super.beforeSuite();

        init(SUITE);
    }

    private void init(Scope scope) {
        if (this.scope != scope) return;

        aerospikeClient = new AerospikeClient(HOST, PORT, true);
        aerospikeClient.maxConnsPerNode = maxConnsPerNode;
        aerospikeClient.connPoolsPerNode = connPoolsPerNode;
        aerospikeClient.connectionTimeout = Dates.s(120);
        aerospikeClient.start();
        aerospikeClient.waitConnectionEstablished();
        asDeleteAll();
    }


    public AerospikeFixture withScope(Scope scope) {
        this.scope = scope;
        
        return this;
    }

    @SneakyThrows
    @Override
    public void afterMethod() {
        shutdown(METHOD);

        super.afterMethod();
    }

    @SneakyThrows
    @Override
    public void afterClass() {
        shutdown(CLASS);

        super.afterClass();
    }

    @SneakyThrows
    @Override
    public void afterSuite() {
        shutdown(SUITE);

        super.afterSuite();
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
