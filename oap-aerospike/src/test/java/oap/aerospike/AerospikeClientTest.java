package oap.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import oap.application.Kernel;
import oap.application.module.Module;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.time.JavaTimeService;
import oap.util.Dates;
import oap.util.Pair;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static oap.aerospike.AerospikeFixture.TEST_NAMESPACE;
import static oap.testng.Asserts.pathOfResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

/**
 * Created by igor.petrenko on 29.05.2019.
 */
@Slf4j
public class AerospikeClientTest extends Fixtures {
    private static final AerospikeFixture aerospikeFixture;

    static {
        aerospikeFixture = suiteFixture( new AerospikeFixture() );
    }

    @BeforeMethod
    public void beforeMethod() {
        System.setProperty( "TMP_PATH", TestDirectoryFixture.testDirectory().toAbsolutePath().toString().replace( '\\', '/' ) );
    }

    @Test
    public void testClient() {
        var kernel = new Kernel( Module.CONFIGURATION.urlsFromClassPath() );
        try {
            kernel.start( pathOfResource( getClass(), "/test-application.conf" ) );

            var client = kernel.<AerospikeClient>service( "oap-aerospike", "aerospike-client-writer" ).get();
            client.waitConnectionEstablished();
            client.deleteAll( "test", "test", 2 );

            var longId = Strings.repeat( "1", 8_000 );

            client.update( "test", "test", longId,
                "v1", false, 1 );

            client.update( "test", "test", longId,
                "v1", true, 1 );

            client.update( "test", "test", longId,
                "v2", 1, 1 );

            var ret = client.get( "test", "test", longId );

            assertThat( ret.successValue.getBoolean( "v1" ) ).isTrue();
            assertThat( ret.successValue.getInt( "v2" ) ).isEqualTo( 1 );


            var records = client.get( List.of( new Key( "test", "test", longId ), new Key( "test", "test", longId ) ) );
            assertThat( records.successValue.length ).isEqualTo( 2 );
            assertThat( records.successValue[0].getBoolean( "v1" ) ).isTrue();
            assertThat( records.successValue[1].getBoolean( "v1" ) ).isTrue();

            var ret2 = client.get( "test", "test", longId, "v2" );
            assertThat( ret2.successValue.getBoolean( "v1" ) ).isFalse();
            assertThat( ret2.successValue.getInt( "v2" ) ).isEqualTo( 1 );

            client.deleteAll( "test", "test", 1 );

            ret = client.get( "test", "test", longId );
            assertThat( ret.failureValue ).isEqualTo( AerospikeClient.State.NOT_FOUND );
        } finally {
            kernel.stop();
        }
    }

    @Test
    public void testStartWithoutAerospike() {
        var kernel = new Kernel( Module.CONFIGURATION.urlsFromClassPath() );
        try {
            assertThatCode( () -> kernel.start( pathOfResource( getClass(), "/test-application.conf" ) ) )
                .doesNotThrowAnyException();
        } finally {
            kernel.stop();
        }
    }

    @Test
    public void testGetSets() throws IOException {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.start();
            client.waitConnectionEstablished();

            client.update( TEST_NAMESPACE, "set1", "id1", "b1", "v1", 1 );
            client.update( TEST_NAMESPACE, "set2", "id1", "b1", "v1", 1 );
            assertThat( client.getSets( TEST_NAMESPACE ).successValue ).contains( "set1", "set2" );
        }
    }

    @Test
    public void testGenerationBins() throws IOException {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.start();
            client.waitConnectionEstablished();

            client.update( TEST_NAMESPACE, "test", "id1", Map.of( "b1", "v1" ), 1 );

            var record = client.get( TEST_NAMESPACE, "test", "id1" )
                .orElseThrow( s -> new RuntimeException( s.name() ) );

            assertThat( client.update( TEST_NAMESPACE, "test", "id1", record.generation, Map.of( "b1", "v1" ), 1 ) ).isEmpty();
            assertThat( client.update( TEST_NAMESPACE, "test", "id1", record.generation, Map.of( "b1", "v2" ), 1 ) ).isPresent();
        }
    }

    private static void setTimeout( AerospikeClient client ) {
        client.writeTimeout = Dates.s( 10 );
        client.readTimeout = Dates.s( 10 );
    }

    @Test
    public void testBatchRead() throws Exception {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.start();
            client.waitConnectionEstablished();

            client.update( TEST_NAMESPACE, "set1", "id1", "b1", "v1", 1 );
            client.update( TEST_NAMESPACE, "set1", "id1", "b2", "v2", 1 );
            client.update( TEST_NAMESPACE, "set2", "id1", "c1", "v3", 1 );

            var result = client.get(
                List.of(
                    new BatchRead( new Key( TEST_NAMESPACE, "set1", "id1" ), new String[] { "b1", "b2" } ),
                    new BatchRead( new Key( TEST_NAMESPACE, "set2", "id1" ), new String[] { "c1" } )
                ),
                false );

            assertThat( result.successValue ).isNotNull();
            assertThat( result.successValue[0].getString( "b1" ) ).isEqualTo( "v1" );
            assertThat( result.successValue[0].getString( "b2" ) ).isEqualTo( "v2" );
            assertThat( result.successValue[1].getString( "c1" ) ).isEqualTo( "v3" );
        }
    }

    @Test
    public void testFindAndModify() throws IOException {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.start();
            client.waitConnectionEstablished();

            assertThat( client.findAndModify( TEST_NAMESPACE, "test", "id1", r -> Map.of( "b1", 1L ), 1, Dates.s( 60 ) ) ).isEmpty();

            var record = client.get( TEST_NAMESPACE, "test", "id1" );
            assertNotNull( record );

            assertThat( client.findAndModify( TEST_NAMESPACE, "test", "id1", r -> {
                assertThat( r.getLong( "b1" ) ).isEqualTo( 1L );
                r.bins.put( "b1", r.getLong( "b1" ) + 1 );

                return r.bins;
            }, 1, Dates.s( 60 ), "b1", "b2" ) ).isEmpty();
            assertThat( client.findAndModify( TEST_NAMESPACE, "test", "id1", r -> {
                assertThat( r.getLong( "b1" ) ).isEqualTo( 2L );
                r.bins.put( "b1", r.getLong( "b1" ) + 1 );

                return r.bins;
            }, 1, Dates.s( 60 ) ) ).isEmpty();

            record = client.get( TEST_NAMESPACE, "test", "id1" );
            assertThat( record.successValue.getLong( "b1" ) ).isEqualTo( 3L );
        }
    }

    @Test
    public void testStream() throws IOException {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.primaryKeyStored = true;
            client.start();
            client.waitConnectionEstablished();

            assertThat( client.stream( TEST_NAMESPACE, "test" ) ).isEmpty();

            client.update( TEST_NAMESPACE, "test", "id1", Map.of( "a", "10" ), 1 );

            try( var s = client.stream( TEST_NAMESPACE, "test" ) ) {
                var list = s.collect( Collectors.toList() );
                assertThat( list ).hasSize( 1 );
                assertThat( list.get( 0 )._1 ).isEqualTo( new Key( TEST_NAMESPACE, "test", "id1" ) );
                assertThat( list.get( 0 )._2.bins ).isEqualTo( Map.of( "a", "10" ) );
            }
        }
    }

    @Test
    public void testQuery() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.primaryKeyStored = true;
            client.start();
            client.waitConnectionEstablished();

            try {
                assertThat( client.createIndex( TEST_NAMESPACE, "test", "test_index", "test_bin", IndexType.NUMERIC, IndexCollectionType.LIST ) ).isEmpty();

                client.update( TEST_NAMESPACE, "test", "id1", "test_bin", List.of( 1, 2, 3 ), 2 );
                client.update( TEST_NAMESPACE, "test", "id2", "test_bin", List.of( 1, 4 ), 2 );
                client.update( TEST_NAMESPACE, "test", "id3", "test_bin", List.of( 10 ), 2 );
                client.update( TEST_NAMESPACE, "test", "id3", "aaa", 1, 2 );


                var res = new ArrayList<Pair<Key, Record>>();
                var resultFuture = client.query( TEST_NAMESPACE, "test", "test_index", Filter.contains( "test_bin", IndexCollectionType.LIST, 1 ), res::add,
                    "test_bin" );
                var result = resultFuture.get( 10, TimeUnit.SECONDS );
                assertThat( result ).isEmpty();
                System.out.println( res );
                assertThat( res ).hasSize( 2 );
            } finally {
                assertThat( client.dropIndex( TEST_NAMESPACE, "test", "test_index" ) ).isEmpty();
            }
        }
    }

    @Test
    public void testOperations() throws IOException, InterruptedException, TimeoutException {
        try( var client = new AerospikeClient( AerospikeFixture.HOST, aerospikeFixture.getPort(), true, JavaTimeService.INSTANCE ) ) {
            setTimeout( client );
            client.primaryKeyStored = true;
            client.eventLoopSize = 1;
            client.maxCommandsInQueue = 1;
            client.maxConnsPerNode = 5;
            client.connPoolsPerNode = 1;

            client.start();
            client.waitConnectionEstablished();

            var counter = new AtomicInteger();

            var ac = client.operations().successValue;
            var key = new Key( TEST_NAMESPACE, "test", "1" );
            var operation = ListOperation.append( "b", new Value.FloatValue( 1 ) );

            for( var i = 0; i < 1000; i++ ) {
                assertThat( ac.operate( new RecordListener() {
                    @Override
                    public void onSuccess( Key key, Record record ) {
                        counter.incrementAndGet();
                        log.trace( "count = {}", counter.get() );
                    }

                    @Override
                    public void onFailure( AerospikeException exception ) {
                        fail();
                    }
                }, key, operation ) ).isEmpty();
            }

            ac.waitTillComplete( 1000, 10, TimeUnit.SECONDS );
            assertThat( counter.get() ).isEqualTo( 1000 );
        }
    }
}
