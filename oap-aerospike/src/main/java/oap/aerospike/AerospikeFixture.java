package oap.aerospike;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import lombok.Getter;
import oap.system.Env;
import oap.testng.AbstractEnvFixture;
import oap.time.JodaTimeService;
import oap.util.Dates;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Env:
 * - AEROSPIKE_TEST_NAMESPACE
 */
public class AerospikeFixture extends AbstractEnvFixture<AerospikeFixture> {
    public static final String TEST_NAMESPACE = Env.get( "AEROSPIKE_TEST_NAMESPACE", "test" );
    public static final String HOST = "localhost";
    private static final String AEROSPIKE_VERSION = "6.2.0.3_3";
    public final int maxConnsPerNode;
    public final int connPoolsPerNode;
    @Getter
    private final int port;
    public AerospikeClient aerospikeClient;
    private GenericContainer<?> container;

    public AerospikeFixture() {
        this( 300, 1 );
    }

    public AerospikeFixture( int maxConnsPerNode, int connPoolsPerNode ) {
        this.maxConnsPerNode = maxConnsPerNode;
        this.connPoolsPerNode = connPoolsPerNode;

        define( "AEROSPIKE_TEST_NAMESPACE", TEST_NAMESPACE );
        define( "AEROSPIKE_HOSTS", "localhost" );
        definePort( "AEROSPIKE_PORT" );
        port = portFor( "AEROSPIKE_PORT" );
    }

    @Override
    protected void before() {
        super.before();

        var portBinding = new PortBinding(
            Ports.Binding.bindPort( port ),
            new ExposedPort( 3000 ) );

        container = new GenericContainer<>( DockerImageName.parse( "aerospike/aerospike-server-enterprise:" + AEROSPIKE_VERSION ) )
            .withCreateContainerCmdModifier( cmd -> cmd.getHostConfig().withPortBindings( portBinding ) );
        container.start();
    }

    public AerospikeClient getAerospikeClient( Integer ttl ) {
        var aerospikeClient = new AerospikeClient( container.getHost(), container.getMappedPort( 3000 ), true, JodaTimeService.INSTANCE );
        aerospikeClient.maxConnsPerNode = maxConnsPerNode;
        aerospikeClient.connPoolsPerNode = connPoolsPerNode;
        aerospikeClient.connectionTimeout = Dates.s( 120 );
        aerospikeClient.start();
        aerospikeClient.waitConnectionEstablished();
        return aerospikeClient;
    }

    @Override
    public void beforeMethod() {
        super.beforeMethod();
        if( container.isRunning() ) {
            aerospikeClient = getAerospikeClient( null );
            asDeleteAll();
        } else {
            throw new RuntimeException( "Aerospike container was shutdown while being used. Check the scope of the fixture." );
        }
    }

    @Override
    public void after() {
        try {
            aerospikeClient.close();
            if( container != null ) {
                container.stop();
            }
        } catch( IOException e ) {
            throw new UncheckedIOException( e );
        }
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
