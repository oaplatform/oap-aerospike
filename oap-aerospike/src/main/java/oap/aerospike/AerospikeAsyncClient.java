package oap.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.WritePolicy;
import lombok.extern.slf4j.Slf4j;
import oap.LogConsolidated;
import oap.time.TimeService;
import oap.util.Dates;
import org.slf4j.event.Level;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by igor.petrenko on 2020-11-20.
 */
@Slf4j
public class AerospikeAsyncClient {
    private final com.aerospike.client.AerospikeClient aerospikeClient;
    private final EventLoops eventLoops;
    private final WritePolicy writePolicy;
    private final TimeService timeService;
    private final LinkedBlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>();
    private final AtomicLong counter = new AtomicLong();

    public AerospikeAsyncClient( com.aerospike.client.AerospikeClient aerospikeClient,
                                 EventLoops eventLoops,
                                 WritePolicy writePolicy,
                                 TimeService timeService ) {
        this.aerospikeClient = aerospikeClient;
        this.eventLoops = eventLoops;
        this.writePolicy = writePolicy;
        this.timeService = timeService;
    }

    public Optional<Integer> operate( RecordListener recordListener, Key key, Operation... operations ) {

        try {
            processQueue( 1 );

            var blockingQueueRecordListener = new BlockingQueueRecordListener( recordListener );

            var proc = new Runnable() {
                @Override
                public void run() {
                    aerospikeClient.operate( eventLoops.next(), blockingQueueRecordListener, writePolicy, key, operations );
                }
            };

            blockingQueueRecordListener.proc = proc;

            proc.run();

            return Optional.empty();
        } catch( AerospikeException e ) {
            LogConsolidated.log( log, Level.ERROR, Dates.s( 5 ), e.getMessage(), null );
            return Optional.of( e.getResultCode() );
        } catch( InterruptedException e ) {
            return Optional.of( AerospikeClient.ERROR_CODE_INTERRUPTED );
        }
    }

    private void processQueue( int sleep ) throws InterruptedException {
        while( !blockingQueue.isEmpty() ) {
            if( sleep > 0 ) Thread.sleep( sleep );
            blockingQueue.poll().run();
        }
    }

    public void waitTillComplete( long count, long timeout, TimeUnit unit ) throws InterruptedException, TimeoutException {
        var start = timeService.currentTimeMillis();

        processQueue( 0 );

        while( counter.get() < count ) {
            if( timeService.currentTimeMillis() - start >= unit.toMillis( timeout ) )
                throw new TimeoutException();

            Thread.sleep( 1 );
            processQueue( 0 );
        }
    }


    private class BlockingQueueRecordListener implements RecordListener {
        private final RecordListener recordListener;
        public Runnable proc;

        private BlockingQueueRecordListener( RecordListener recordListener ) {
            this.recordListener = recordListener;
        }

        @Override
        public void onSuccess( Key key, Record record ) {
            recordListener.onSuccess( key, record );
            counter.incrementAndGet();
        }

        @Override
        public void onFailure( AerospikeException exception ) {
            if( exception instanceof AerospikeException.AsyncQueueFull ) {
                blockingQueue.add( proc );
            } else {
                recordListener.onFailure( exception );
            }
        }
    }
}
