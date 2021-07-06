package oap.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import oap.LogConsolidated;
import oap.concurrent.Threads;
import oap.io.Closeables;
import oap.time.TimeService;
import oap.util.Dates;
import oap.util.Pair;
import oap.util.Result;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.slf4j.event.Level;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Integer.max;
import static java.util.stream.Collectors.toList;
import static oap.util.Dates.s;
import static oap.util.Pair.__;

/**
 * Created by igor.petrenko on 29.05.2019.
 */
@Slf4j
public class AerospikeClient implements Closeable {
    public static final int ERROR_CODE_UNKNOWN = -10101010;
    public static final int ERROR_CODE_CLIENT_TIMEOUT = -10101011;
    public static final int ERROR_CODE_REJECTED = -10101012;
    public static final int ERROR_CODE_INTERRUPTED = -10101013;
    public static final Counter METRIC_READ_SUCCESS = Metrics.counter( "aerospike_client", Tags.of( "type", "read", "status", "success", "code", "unknown" ) );
    public static final Counter METRIC_READ_BATCH_SUCCESS = Metrics.counter( "aerospike_client", Tags.of( "type", "read_batch", "status", "success", "code", "unknown" ) );
    public static final Counter METRIC_WRITE_SUCCESS = Metrics.counter( "aerospike_client", Tags.of( "type", "write", "status", "success", "code", "unknown" ) );
    private static final Pair<Key, Record> END = __( new Key( "", "", "" ), new Record( Map.of(), -1, -1 ) );

    static {
        AerospikeLog.init( Log.Level.ERROR );
    }


    public final InfoPolicy infoPolicy;
    private final List<String> hosts;
    private final boolean forceSingleNode;
    private final TimeService timeService;
    private final int port;
    private final Lock connectionLock = new ReentrantLock();
    public int maxConnsPerNode = 300;
    public int connPoolsPerNode = 1;
    public long connectionTimeout = 100;
    public long readTimeout = 20;
    public long writeTimeout = Dates.s( 1 );
    public long batchTimeout = 40;
    public long timeoutDelay = 3000;
    public long indexTimeout = Dates.s( 60 );
    public int eventLoopSize = 0;
    public int commandsPerEventLoop = 256;
    public int maxCommandsInQueue = 0;
    public int queueInitialCapacity = 256;
    public long minTimeout = 100;
    public boolean primaryKeyStored = false;
    public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;
    public ClientPolicy clientPolicy;
    public Policy readPolicy;
    public WritePolicy writePolicy;
    public BatchPolicy batchPolicy;
    public int rackId = 0;
    public EventLoops eventLoops;
    private volatile com.aerospike.client.AerospikeClient baseClient;
    private AerospikeCluster cluster;
    private EventPolicy eventPolicy;

    public AerospikeClient( String hosts, int port, boolean forceSingleNode, TimeService timeService ) {
        this.hosts = List.of( StringUtils.split( hosts, ',' ) ).stream().map( String::trim ).collect( toList() );
        this.forceSingleNode = forceSingleNode;
        this.timeService = timeService;
        infoPolicy = new InfoPolicy();
        this.port = port;
    }

    public static String getResultString( int code ) {
        return switch( code ) {
            case ERROR_CODE_UNKNOWN -> "unknown";
            case ERROR_CODE_CLIENT_TIMEOUT -> "Client timeout";
            case ERROR_CODE_REJECTED -> "Rejected";
            case ERROR_CODE_INTERRUPTED -> "Interrupted";
            default -> ResultCode.getResultString( code );
        };
    }

    private static Counter getMetricWriteError( int code ) {
        return Counter.builder( "aerospike_client" ).tags( "type", "write", "status", "error", "code", getResultString( code ) ).register( Metrics.globalRegistry );
    }

    private static Counter getMetricReadError( int code ) {
        return Counter.builder( "aerospike_client" ).tags( "type", "read", "status", "error", "code", getResultString( code ) ).register( Metrics.globalRegistry );
    }

    public void start() {
        var availableProcessors = Runtime.getRuntime().availableProcessors();
        if( connPoolsPerNode == 0 ) connPoolsPerNode = max( availableProcessors / 8, 1 );

        if( eventLoopSize == 0 ) eventLoopSize = availableProcessors;

        clientPolicy = new ClientPolicy();

        writePolicy = new WritePolicy( clientPolicy.writePolicyDefault );
        writePolicy.totalTimeout = ( int ) writeTimeout;
        writePolicy.setTimeout( ( int ) writeTimeout );
        writePolicy.timeoutDelay = ( int ) timeoutDelay;
        writePolicy.sendKey = primaryKeyStored;
        writePolicy.commitLevel = commitLevel;

        batchPolicy = new BatchPolicy( clientPolicy.batchPolicyDefault );
        batchPolicy.setTimeout( ( int ) batchTimeout );
        batchPolicy.timeoutDelay = ( int ) timeoutDelay;
        batchPolicy.sendKey = primaryKeyStored;

        readPolicy = new Policy( clientPolicy.readPolicyDefault );
        readPolicy.setTimeout( ( int ) readTimeout );
        readPolicy.timeoutDelay = ( int ) timeoutDelay;
        readPolicy.sendKey = primaryKeyStored;

        clientPolicy.maxConnsPerNode = maxConnsPerNode;
        clientPolicy.connPoolsPerNode = connPoolsPerNode;
        clientPolicy.timeout = ( int ) connectionTimeout;
        clientPolicy.failIfNotConnected = true;
        clientPolicy.forceSingleNode = forceSingleNode;
        clientPolicy.rackId = rackId;

        eventPolicy = new EventPolicy();
        eventPolicy.commandsPerEventLoop = commandsPerEventLoop;
        eventPolicy.maxCommandsInProcess = maxConnsPerNode / eventLoopSize;
        eventPolicy.maxCommandsInQueue = maxCommandsInQueue;
        eventPolicy.queueInitialCapacity = queueInitialCapacity;
        eventPolicy.minTimeout = ( int ) minTimeout;

        log.info( "hosts: {}:{}, forceSingleNode: {}, rackId: {}, availableProcessors: {}, maxConnsPerNode:{}, connPoolsPerNode:{}",
            hosts, port, forceSingleNode, rackId, availableProcessors, maxConnsPerNode, connPoolsPerNode );

        log.info( "eventLoopSize: {}, maxCommandsInProcess: {}, maxCommandsInQueue: {}, queueInitialCapacity:{}, commandsPerEventLoop:{}, minTimeout:{}",
            eventLoopSize, maxConnsPerNode / eventLoopSize,
            maxCommandsInQueue, queueInitialCapacity, commandsPerEventLoop, Dates.durationToString( minTimeout ) );

        clientPolicy.eventLoops = eventLoops = new NioEventLoops( eventPolicy, eventLoopSize, true, "aerospike" );

        if( rackId > 0 ) {
            writePolicy.replica = Replica.PREFER_RACK;
            batchPolicy.replica = Replica.PREFER_RACK;
            readPolicy.replica = Replica.PREFER_RACK;

            clientPolicy.rackAware = true;
        }

        Metrics.gauge( "aerospike_client_pool", Tags.of( "type", "user" ), this,
            ac -> Stream.of( ac.eventLoops.getArray() ).mapToLong( EventLoop::getProcessSize ).sum() );
    }

    @SneakyThrows
    public void waitConnectionEstablished() {
        while( !getConnection().isSuccess() ) {
            Threads.sleepSafely( 100 );
        }
    }

    public Result<com.aerospike.client.AerospikeClient, AerospikeException> getConnection() throws AerospikeException {
        if( baseClient == null ) {
            if( connectionLock.tryLock() ) {
                try {
                    if( baseClient == null ) {
                        log.trace( "connecting..." );

                        var hosts = this.hosts.stream().map( h -> new Host( h, port ) ).toArray( Host[]::new );
                        try {
                            baseClient = new com.aerospike.client.AerospikeClient( clientPolicy, hosts );
                            var nodes = baseClient.getNodes();
                            log.info( "nodes = {}", Stream.of( nodes ).map( Node::getHost ).collect( toList() ) );

                            cluster = new AerospikeCluster( forceSingleNode, clientPolicy.tendInterval, baseClient );

                            log.trace( "connecting... Done" );
                        } catch( AerospikeException e ) {
                            LogConsolidated.log( log, Level.TRACE, s( 5 ), e.getMessage(), e );
                            return Result.failure( e );
                        }
                    }
                } finally {
                    connectionLock.unlock();
                }
            }
        }
        return Result.success( baseClient );
    }

    private WritePolicy getWritePolicyWithGeneration( WritePolicy writePolicy, int generation ) {
        var currentWritePolicy = writePolicy;
        if( generation >= 0 ) {
            currentWritePolicy = new WritePolicy( this.writePolicy );
            currentWritePolicy.generation = generation;
            currentWritePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        }
        return currentWritePolicy;
    }


    public void update( String namespace, String set, String id, Map<String, Object> bins, int retry ) {
        update( writePolicy, new Key( namespace, set, id ), -1, bins, retry );
    }

    public void update( WritePolicy writePolicy, Key id, Map<String, Object> bins, int retry ) {
        update( writePolicy, id, -1, bins, retry );
    }

    public void update( Key key, String beanName, Object value, int retry ) {
        update( key, Map.of( beanName, value ), retry );
    }

    public void update( String namespace, String set, byte[] id, String beanName, Object value, int retry ) {
        update( new Key( namespace, set, id ), Map.of( beanName, value ), retry );
    }

    public void update( WritePolicy writePolicy, String namespace, String set, String id, String beanName, Object value, int retry ) {
        update( writePolicy, new Key( namespace, set, id ), Map.of( beanName, value ), retry );
    }

    public void update( String namespace, String set, String id, String beanName, Object value, int retry ) {
        update( writePolicy, namespace, set, id, beanName, value, retry );
    }

    public void update( String namespace, String set, String id, int generation, String beanName, Object value, int retry ) {
        update( writePolicy, new Key( namespace, set, id ), generation, Map.of( beanName, value ), retry );
    }

    public void update( String namespace, String set, byte[] id, Map<String, Object> bins, int retry ) {
        update( writePolicy, new Key( namespace, set, id ), -1, bins, retry );
    }

    public Optional<Integer> update( String namespace, String set, String id, int generation, Map<String, Object> bins, int retry ) {
        return update( writePolicy, new Key( namespace, set, id ), generation, bins, retry );
    }

    public Optional<Integer> update( String namespace, String set, byte[] id, int generation, Map<String, Object> bins, int retry ) {
        return update( writePolicy, new Key( namespace, set, id ), generation, bins, retry );
    }

    Optional<Integer> update( WritePolicy writePolicy, Key key, int generation, Map<String, Object> bins, int retry ) {
        var arr = new Bin[bins.size()];
        var i = 0;
        for( var entry : bins.entrySet() ) {
            arr[i++] = new Bin( entry.getKey(), entry.getValue() );
        }
        var currentWritePolicy = getWritePolicyWithGeneration( writePolicy, generation );

        var ret = getConnectionAndUpdate( generation, ( connection, eventLoop ) -> {
            try {
                var wp = new WritePolicy( currentWritePolicy );
                wp.maxRetries = retry;
                var listener = new CompletableFutureWriteListener();
                connection.put( eventLoop, listener, wp, key, arr );
                return listener.completableFuture
                    .handle( ( r, e ) -> {
                        if( e != null ) {
                            return Optional.of( getResultCode( e ) );
                        } else {
                            return Optional.<Integer>empty();
                        }
                    } );
            } catch( AerospikeException e ) {
                return CompletableFuture.failedFuture( e );
            }
        }, writeTimeout );
        if( ret.isEmpty() ) METRIC_WRITE_SUCCESS.increment();
        else getMetricWriteError( ret.get() ).increment();


        return ret;
    }

    public void update( Key id, Map<String, Object> bins, int retry ) {
        update( writePolicy, id, bins, retry );
    }

    public Optional<Integer> createIndex( String namespace, String set, String indexName, String binName,
                                          IndexType indexType ) {
        return createIndex( namespace, set, indexName, binName, indexType, IndexCollectionType.DEFAULT );

    }

    public Optional<Integer> createIndex( String namespace, String set, String indexName, String binName,
                                          IndexType indexType, IndexCollectionType indexCollectionType ) {
        var connection = getConnection();

        if( !connection.isSuccess() ) return Optional.of( connection.failureValue.getResultCode() );

        try {
            log.info( "creating index {}/{}/{} on bin {}...", namespace, set, indexName, binName );
            var policy = new Policy();
            policy.setTimeout( ( int ) indexTimeout );
            var task = connection.successValue.createIndex( policy, namespace, set, indexName, binName, indexType, indexCollectionType );
            task.waitTillComplete();
            log.info( "creating index {}/{}/{} on bin {}... Done.", namespace, set, indexName, binName );
        } catch( AerospikeException e ) {
            log.error( e.getMessage(), e );
            return Optional.of( e.getResultCode() );
        }

        return Optional.empty();
    }

    public Optional<Integer> dropIndex( String namespace, String set, String indexName ) {
        var connection = getConnection();

        if( !connection.isSuccess() ) return Optional.of( connection.failureValue.getResultCode() );

        try {
            log.info( "droppping index {}/{}/{}...", namespace, set, indexName );
            var policy = new Policy();
            policy.setTimeout( ( int ) indexTimeout );
            var task = connection.successValue.dropIndex( policy, namespace, set, indexName );
            task.waitTillComplete();
            log.info( "droppping index {}/{}/{}... Done.", namespace, set, indexName );
        } catch( AerospikeException e ) {
            log.error( e.getMessage(), e );
            return Optional.of( e.getResultCode() );
        }

        return Optional.empty();
    }

    public Optional<Integer> findAndModify( String namespace, String set, String id, Function<Record, Map<String, Object>> func, int maxRetries, long timeout, String... binNames ) {
        var policy = new Policy( readPolicy );
        policy.setTimeout( ( int ) timeout );
        policy.maxRetries = maxRetries;

        var key = new Key( namespace, set, id );

        try {
            var readListener = new CompletableFutureRecordListener();
            var connection = getConnection();
            if( !connection.isSuccess() ) return Optional.of( connection.failureValue.getResultCode() );

            connection.successValue.get( eventLoops.next(), readListener, policy, key,
                binNames.length == 0 ? null : binNames );

            return readListener.completableFuture
                .thenCompose( record -> {
                    var writeListener = new CompletableFutureWriteListener();

                    METRIC_READ_SUCCESS.increment();

                    var bins = func.apply( record );

                    var arr = new Bin[bins.size()];
                    var i = 0;
                    for( var entry : bins.entrySet() ) {
                        arr[i++] = new Bin( entry.getKey(), entry.getValue() );
                    }
                    var currentWritePolicy = getWritePolicyWithGeneration( writePolicy,
                        record != null ? record.generation : -1 );
                    currentWritePolicy.setTimeout( ( int ) timeout );
                    currentWritePolicy.maxRetries = maxRetries;

                    try {
                        connection.successValue.put( eventLoops.next(), writeListener, currentWritePolicy, new Key( namespace, set, id ), arr );

                        return writeListener.completableFuture
                            .thenApply( r -> {
                                METRIC_WRITE_SUCCESS.increment();
                                return Optional.<Integer>empty();
                            } )
                            .exceptionally( e -> {
                                var resultCode = getResultCode( e );
                                getMetricWriteError( resultCode ).increment();

                                return Optional.of( resultCode );
                            } );
                    } catch( AerospikeException.AsyncQueueFull e ) {
                        getMetricReadError( ERROR_CODE_REJECTED ).increment();
                        return CompletableFuture.completedStage( Optional.of( ERROR_CODE_REJECTED ) );
                    }
                } )
                .exceptionally( e -> {
                    var resultCode = getResultCode( e );
                    getMetricReadError( resultCode ).increment();

                    return Optional.of( resultCode );
                } )
                .get( timeout, TimeUnit.MILLISECONDS );
        } catch( AerospikeException.AsyncQueueFull e ) {
            getMetricReadError( ERROR_CODE_REJECTED ).increment();
            return Optional.of( ERROR_CODE_REJECTED );
        } catch( InterruptedException | ExecutionException e ) {
            getMetricReadError( ERROR_CODE_UNKNOWN ).increment();
            return Optional.of( ERROR_CODE_UNKNOWN );
        } catch( TimeoutException e ) {
            getMetricReadError( ERROR_CODE_CLIENT_TIMEOUT ).increment();
            return Optional.of( ERROR_CODE_CLIENT_TIMEOUT );
        }
    }

    private int getResultCode( Throwable e ) {
        if( e instanceof AerospikeException ) return ( ( AerospikeException ) e ).getResultCode();

        return ERROR_CODE_UNKNOWN;
    }

    public Result<Set<String>, State> getSets( String namespace ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            var sets = new HashSet<String>();

            for( var node : connection.getNodes() ) {
                var info = Info.request( node, "sets/" + namespace );
                var setInfos = StringUtils.split( info, ';' );
                for( var setInfo : setInfos ) {
                    var parameters = StringUtils.split( setInfo, ':' );
                    for( var parameter : parameters ) {
                        if( parameter.startsWith( "set=" ) ) {
                            sets.add( parameter.substring( "set=".length() ) );
                            break;
                        }
                    }
                }
            }
            return CompletableFuture.completedFuture( sets );
        }, readTimeout );
    }

    public <T> Result<T, State> getConnectionAndGetRecord( AerospikeClientFunction<CompletableFuture<T>> func, long readTimeout ) {
        try {
            var connection = getConnection();
            if( !connection.isSuccess() ) return Result.failure( State.ERROR );

            var res = func.apply( connection.successValue, eventLoops.next() );

            return res
                .<Result<T, State>>thenApply( record -> {
                    if( record == null ) return Result.failure( State.NOT_FOUND );

                    return Result.success( record );
                } )
                .exceptionally(
                    e -> {
                        getMetricReadError( getResultCode( e ) ).increment();
                        return Result.failure( State.ERROR );
                    } )
                .get( readTimeout, TimeUnit.MILLISECONDS );
        } catch( AerospikeException.AsyncQueueFull e ) {
            getMetricReadError( ERROR_CODE_REJECTED ).increment();
        } catch( TimeoutException e ) {
            getMetricReadError( ERROR_CODE_CLIENT_TIMEOUT ).increment();
        } catch( Exception e ) {
            getMetricReadError( ERROR_CODE_UNKNOWN ).increment();
        }

        return Result.failure( State.ERROR );
    }

    public Optional<Integer> getConnectionAndUpdate( int generation, AerospikeClientFunction<CompletableFuture<Optional<Integer>>> func, long timeout ) {
        try {
            var connection = getConnection();
            if( !connection.isSuccess() ) return Optional.of( connection.failureValue.getResultCode() );
            var res = func.apply( connection.successValue, eventLoops.next() );
            return res.get( timeout, TimeUnit.MILLISECONDS );
        } catch( Throwable e ) {
            return registerException( e );
        }
    }

    private Optional<Integer> registerException( Throwable ex ) {
        if( ex instanceof ExecutionException ) {
            return registerException( ex.getCause() );
        } else if( ex instanceof InterruptedException ) {
            getMetricWriteError( ERROR_CODE_INTERRUPTED ).increment();
            return Optional.of( ERROR_CODE_INTERRUPTED );
        } else if( ex instanceof AerospikeException.AsyncQueueFull ) {
            getMetricWriteError( ERROR_CODE_REJECTED ).increment();
            return Optional.of( ERROR_CODE_REJECTED );
        } else if( ex instanceof AerospikeException ) {
            var aerospikeException = ( AerospikeException ) ex;
            getMetricWriteError( aerospikeException.getResultCode() ).increment();
            return Optional.of( aerospikeException.getResultCode() );
        } else if( ex instanceof TimeoutException ) {
            getMetricWriteError( ERROR_CODE_CLIENT_TIMEOUT ).increment();
            return Optional.of( ERROR_CODE_CLIENT_TIMEOUT );
        } else {
            LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
            getMetricWriteError( ERROR_CODE_UNKNOWN ).increment();
            return Optional.of( ERROR_CODE_UNKNOWN );
        }
    }

    public Result<Record, State> get( String namespace, String set, String id ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            var listener = new CompletableFutureRecordListener();
            connection.get( eventLoop, listener, readPolicy, new Key( namespace, set, id ) );
            return listener.completableFuture;
        }, readTimeout ).ifSuccess( r -> METRIC_READ_SUCCESS.increment() );
    }

    public Result<Record, State> get( String namespace, String set, String id, String... binNames ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            var listener = new CompletableFutureRecordListener();
            connection.get( eventLoop, listener, readPolicy, new Key( namespace, set, id ), binNames );
            return listener.completableFuture;
        }, readTimeout ).ifSuccess( r -> METRIC_READ_SUCCESS.increment() );
    }

    public Result<Record[], State> get( Key keyf, Key... keys ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            var a = new Key[keys.length + 1];
            a[0] = keyf;
            System.arraycopy( keys, 0, a, 1, keys.length );
            var listener = new CompletableFutureRecordSequenceListener( a );
            connection.get( eventLoop, listener, batchPolicy, a );
            return listener.completableFuture;
        }, readTimeout ).ifSuccess( r -> METRIC_READ_SUCCESS.increment() );
    }

    public Result<Record[], State> get( List<Key> keys ) {
        if( keys.isEmpty() ) return Result.success( new Record[0] );

        return get( keys.get( 0 ), keys.subList( 1, keys.size() ).toArray( new Key[keys.size() - 1] ) );
    }

    public Result<Record, State> get( long timeout, Key key ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            var policy = new Policy( readPolicy );
            policy.setTimeout( ( int ) timeout );

            var listener = new CompletableFutureRecordListener();
            connection.get( eventLoop, listener, policy, key );
            return listener.completableFuture;
        }, readTimeout ).ifSuccess( r -> METRIC_READ_SUCCESS.increment() );
    }

    public Result<Record, State> get( Key key ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            var listener = new CompletableFutureRecordListener();
            connection.get( eventLoop, listener, readPolicy, key );
            return listener.completableFuture;
        }, readTimeout ).ifSuccess( r -> METRIC_READ_SUCCESS.increment() );
    }

    /**
     * Asynchronously read multiple records for specified batch keys in one batch call.
     * This method registers the command with an event loop and returns.
     * The event loop thread will process the command and send the results to the listener.
     * <p>
     * This method allows different namespaces/bins to be requested for each key in the batch.
     * The returned records are located in the same list.
     * If the BatchRead key field is not found, the corresponding record field will be null.
     * The policy can be used to specify timeouts.
     * <p>
     * If `strict` is `true`   - a batch request to a node fails, the entire batch is cancelled.
     * If `strict` is `false`  - a batch request to a node fails, responses from other nodes will continue to
     * be processed
     */
    public Result<Record[], State> get( List<BatchRead> batchReads, boolean strict ) {
        return getConnectionAndGetRecord( ( connection, eventLoop ) -> {
            if( strict ) {
                var listener = new CompletableFutureBatchListListener();
                connection.get( eventLoop, listener, batchPolicy, batchReads );
                return listener.completableFuture;
            } else {
                var listener = new CompletableFutureBatchSequenceListener( batchReads );
                connection.get( eventLoop, listener, batchPolicy, batchReads );
                return listener.completableFuture;
            }
        }, batchTimeout ).ifSuccess( r -> METRIC_READ_BATCH_SUCCESS.increment() );
    }

    public CompletableFuture<Optional<Integer>> query( String namespace, String set, String indexName, Filter filter, Consumer<Pair<Key, Record>> func, String... binNames ) {
        var f = new CompletableFuture<Optional<Integer>>();
        var connection = getConnection();
        if( !connection.isSuccess() ) {
            f.complete( Optional.of( connection.failureValue.getResultCode() ) );
            return f;
        }

        var statement = new Statement();
        statement.setNamespace( namespace );
        statement.setSetName( set );
        statement.setFilter( filter );
        statement.setIndexName( indexName );
        statement.setBinNames( binNames );

        try {
            connection.successValue.query( eventLoops.next(), new RecordSequenceListener() {
                @Override
                public void onRecord( Key key, Record record ) throws AerospikeException {
                    METRIC_READ_SUCCESS.increment();

                    func.accept( __( key, record ) );
                }

                @Override
                public void onSuccess() {
                    f.complete( Optional.empty() );
                }

                @Override
                public void onFailure( AerospikeException exception ) {
                    getMetricReadError( exception.getResultCode() ).increment();
                    f.complete( Optional.of( exception.getResultCode() ) );
                }
            }, null, statement );
            return f;
        } catch( AerospikeException e ) {
            LogConsolidated.log( log, Level.ERROR, s( 5 ), e.getMessage(), e );
            getMetricReadError( e.getResultCode() ).increment();
            f.complete( Optional.of( e.getResultCode() ) );
            return f;
        }
    }

    public Optional<Integer> deleteAll( String namespace, String set, int retry ) {
        var scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false;

        var throttle = new Throttles( eventLoopSize, eventPolicy.commandsPerEventLoop );

        var wp = new WritePolicy( writePolicy );
        wp.maxRetries = retry;
        return getConnectionAndUpdate( -1, ( connection, eventLoop ) -> {
            var count = new AtomicLong();
            connection.scanAll( scanPolicy, namespace, set, ( key, record ) -> {
                count.incrementAndGet();

                var eventLoop1 = eventLoops.next();
                int index = eventLoop1.getIndex();
                if( throttle.waitForSlot( index, 1 ) ) {
                    log.trace( "delete key {}", key );
                    connection.delete( eventLoop1, new DeleteListener() {
                        @Override
                        public void onSuccess( Key key, boolean existed ) {
                            count.decrementAndGet();
                            throttle.addSlot( index, 1 );
                        }

                        @Override
                        public void onFailure( AerospikeException exception ) {
                            count.decrementAndGet();
                            throttle.addSlot( index, 1 );
                        }
                    }, wp, key );
                }
            } );


            return CompletableFuture.supplyAsync( () -> {
                try {
                    while( count.get() > 0 ) {
                        Thread.sleep( 1 );
                    }
                } catch( InterruptedException ignored ) {
                }

                return Optional.empty();
            } );
        }, writeTimeout );
    }

    public Result<AerospikeAsyncClient, AerospikeException> operations() {
        var connectionResult = getConnection();
        return connectionResult.mapSuccess( ac -> new AerospikeAsyncClient( ac, eventLoops, writePolicy, timeService ) );
    }

    public Stream<Pair<Key, Record>> stream( String namespace, String set ) throws AerospikeException {
        var scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = true;

        var queue = new SynchronousQueue<Pair<Key, Record>>();

        var executorService = Executors.newSingleThreadExecutor();

        var ex = new MutableObject<AerospikeException>();

        try {
            executorService.submit( () -> {
                try {
                    var connection = getConnection();
                    if( !connection.isSuccess() ) throw connection.failureValue;

                    connection.successValue.scanAll( scanPolicy, namespace, set, ( key, record ) -> {
                        try {
                            queue.put( __( key, record ) );
                        } catch( InterruptedException e ) {
                            throw new AerospikeException( e );
                        }
                    } );
                } catch( AerospikeException e ) {
                    ex.setValue( e );
                }

                try {
                    queue.put( END );
                } catch( InterruptedException e ) {
                    ex.setValue( new AerospikeException( e ) );
                }
            } );
        } catch( RejectedExecutionException e ) {
            ex.setValue( new AerospikeException( ERROR_CODE_REJECTED, e ) );
        }

        Stream<Pair<Key, Record>> stream = StreamSupport.stream( Spliterators.spliteratorUnknownSize( new Iterator<>() {
            private Pair<Key, Record> v;

            @Override
            public boolean hasNext() {
                if( ex.getValue() != null ) throw ex.getValue();
                if( v == END ) return false;
                if( v != null ) return true;
                try {
                    v = queue.take();
                } catch( InterruptedException e ) {
                    throw new AerospikeException( e );
                }
                return v != END;
            }

            @Override
            public Pair<Key, Record> next() {
                if( !hasNext() )
                    throw new NoSuchElementException();

                if( ex.getValue() != null ) throw ex.getValue();
                var a = v;
                v = null;
                return a;
            }
        }, 0 ), false );
        return stream.onClose( executorService::shutdownNow );
    }

    @Override
    public void close() throws IOException {
        Closeables.close( cluster );
        Closeables.close( baseClient );
    }

    public enum State {
        SUCCESS, NOT_FOUND, ERROR
    }

    private static class CompletableFutureRecordListener implements RecordListener {
        public final CompletableFuture<Record> completableFuture = new CompletableFuture<>();

        @Override
        public void onSuccess( Key key, Record record ) {
            completableFuture.complete( record );
        }

        @Override
        public void onFailure( AerospikeException exception ) {
            LogConsolidated.log( log, Level.TRACE, s( 5 ), exception.getMessage(), exception );
            completableFuture.completeExceptionally( exception );
        }
    }

    private static class CompletableFutureBatchListListener implements BatchListListener {
        public final CompletableFuture<Record[]> completableFuture = new CompletableFuture<>();

        @Override
        public void onSuccess( List<BatchRead> records ) {
            var array = oap.util.Stream.of( records ).map( r -> r.record ).toArray();
            completableFuture.complete( ( Record[] ) array );
        }

        @Override
        public void onFailure( AerospikeException exception ) {
            LogConsolidated.log( log, Level.TRACE, s( 5 ), exception.getMessage(), exception );
            completableFuture.completeExceptionally( exception );
        }
    }

    private static class CompletableFutureBatchSequenceListener implements BatchSequenceListener {
        public final CompletableFuture<Record[]> completableFuture = new CompletableFuture<>();
        private final Record[] records;
        private final ArrayListMultimap<BatchRead, Integer> keys = ArrayListMultimap.create();

        private CompletableFutureBatchSequenceListener( List<BatchRead> batchReads ) {
            this.records = new Record[batchReads.size()];
            for( var i = 0; i < batchReads.size(); i++ ) {
                this.keys.put( batchReads.get( i ), i );
            }
        }

        @Override
        public void onRecord( BatchRead record ) {
            var index = keys.get( record );
            Preconditions.checkNotNull( index );
            index.forEach( idx -> records[idx] = record.record );
        }

        @Override
        public void onSuccess() {
            completableFuture.complete( records );
        }

        @Override
        public void onFailure( AerospikeException exception ) {
            LogConsolidated.log( log, Level.TRACE, s( 5 ), exception.getMessage(), exception );
            completableFuture.completeExceptionally( exception );
        }
    }

    private static class CompletableFutureRecordSequenceListener implements RecordSequenceListener {
        public final CompletableFuture<Record[]> completableFuture = new CompletableFuture<>();
        private final Record[] records;
        private final ArrayListMultimap<Key, Integer> keys = ArrayListMultimap.<Key, Integer>create();

        private CompletableFutureRecordSequenceListener( Key[] keys ) {
            this.records = new Record[keys.length];

            for( var i = 0; i < keys.length; i++ ) {
                this.keys.put( keys[i], i );
            }
        }

        @Override
        public void onRecord( Key key, Record record ) throws AerospikeException {
            var index = keys.get( key );
            Preconditions.checkNotNull( index );

            index.forEach( idx -> records[idx] = record );
        }

        @Override
        public void onSuccess() {
            completableFuture.complete( records );
        }

        @Override
        public void onFailure( AerospikeException exception ) {
            LogConsolidated.log( log, Level.TRACE, s( 5 ), exception.getMessage(), exception );
            completableFuture.completeExceptionally( exception );
        }
    }

    private static class CompletableFutureWriteListener implements WriteListener {
        public final CompletableFuture<Key> completableFuture = new CompletableFuture<>();

        @Override
        public void onSuccess( Key key ) {
            completableFuture.complete( key );
        }

        @Override
        public void onFailure( AerospikeException exception ) {
            LogConsolidated.log( log, Level.TRACE, s( 5 ), exception.getMessage(), exception );
            completableFuture.completeExceptionally( exception );
        }
    }
}
