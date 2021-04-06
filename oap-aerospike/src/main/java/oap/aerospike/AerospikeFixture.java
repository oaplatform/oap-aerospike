package oap.aerospike;

import oap.system.Env;
import oap.testng.EnvFixture;
import oap.time.JodaTimeService;
import oap.util.Dates;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Env:
 * - AEROSPIKE_TEST_NAMESPACE
 * - AEROSPIKE_HOSTS
 * - AEROSPIKE_PORT
 */
public class AerospikeFixture extends EnvFixture<AerospikeFixture> {
    public static final String TEST_NAMESPACE = Env.get( "AEROSPIKE_TEST_NAMESPACE", "test" );
    public static final String HOST = Env.get( "AEROSPIKE_HOSTS", "localhost" );
    public static final int PORT = Integer.parseInt( Env.get( "AEROSPIKE_PORT", "3000" ) );
    public final int maxConnsPerNode;
    public final int connPoolsPerNode;
    public AerospikeClient aerospikeClient;

    public AerospikeFixture() {
        this( 300, 1 );
    }

    public AerospikeFixture( int maxConnsPerNode, int connPoolsPerNode ) {
        this.maxConnsPerNode = maxConnsPerNode;
        this.connPoolsPerNode = connPoolsPerNode;

        define( "AEROSPIKE_TEST_NAMESPACE", TEST_NAMESPACE );
        define( "AEROSPIKE_HOSTS", HOST );
        define( "AEROSPIKE_PORT", PORT );
    }

    @Override
    protected void before() {
        super.before();

        aerospikeClient = new AerospikeClient( HOST, PORT, true, JodaTimeService.INSTANCE );
        aerospikeClient.maxConnsPerNode = maxConnsPerNode;
        aerospikeClient.connPoolsPerNode = connPoolsPerNode;
        aerospikeClient.connectionTimeout = Dates.s( 120 );
        aerospikeClient.start();
        aerospikeClient.waitConnectionEstablished();
        asDeleteAll();
    }

    @Override
    protected void after() {
        try {
            asDeleteAll();
            aerospikeClient.close();
        } catch( IOException e ) {
            throw new UncheckedIOException( e );
        }

        super.after();
    }

    public void asDeleteAll() {
        var ret = aerospikeClient.getSets( TEST_NAMESPACE );
        ret.ifSuccess( sets -> {
            for( var set : sets ) {
                aerospikeClient.deleteAll( TEST_NAMESPACE, set, 2 );
            }
        } );
    }

    public AerospikeClient getClient() {
        return aerospikeClient;
    }
}
