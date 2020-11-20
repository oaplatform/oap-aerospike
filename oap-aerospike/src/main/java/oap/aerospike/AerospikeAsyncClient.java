package oap.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.WritePolicy;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by igor.petrenko on 2020-11-20.
 */
@Slf4j
public class AerospikeAsyncClient {
    private final com.aerospike.client.AerospikeClient aerospikeClient;
    private final EventLoops eventLoops;
    private final WritePolicy writePolicy;
    private final LinkedBlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>();
    private AtomicLong counter = new AtomicLong();

    public AerospikeAsyncClient(com.aerospike.client.AerospikeClient aerospikeClient, EventLoops eventLoops, WritePolicy writePolicy) {
        this.aerospikeClient = aerospikeClient;
        this.eventLoops = eventLoops;
        this.writePolicy = writePolicy;
    }

    public Optional<Integer> operate(RecordListener recordListener, Key key, Operation... operations) {

        try {
            processQueue(1);

            var blockingQueueRecordListener = new BlockingQueueRecordListener(recordListener);

            var proc = new Runnable() {
                @Override
                public void run() {
                    aerospikeClient.operate(eventLoops.next(), blockingQueueRecordListener, writePolicy, key, operations);
                }
            };

            blockingQueueRecordListener.proc = proc;

            proc.run();

            return Optional.empty();
        } catch (AerospikeException e) {
            log.error(e.getMessage());
            return Optional.of(e.getResultCode());
        } catch (InterruptedException e) {
            return Optional.of(AerospikeClient.ERROR_CODE_INTERRUPTED);
        }
    }

    private void processQueue(int sleep) throws InterruptedException {
        while (!blockingQueue.isEmpty()) {
            if (sleep > 0) Thread.sleep(sleep);
            blockingQueue.poll().run();
        }
    }

    public void waitTillComplete(long count) throws InterruptedException {
        processQueue(0);

        while (counter.get() < count) {
            Thread.sleep(1);
            processQueue(0);
        }
    }


    private class BlockingQueueRecordListener implements RecordListener {
        private final RecordListener recordListener;
        public Runnable proc;

        public BlockingQueueRecordListener(RecordListener recordListener) {
            this.recordListener = recordListener;
        }

        @Override
        public void onSuccess(Key key, Record record) {
            recordListener.onSuccess(key, record);
            counter.incrementAndGet();
        }

        @Override
        public void onFailure(AerospikeException exception) {
            if (exception instanceof AerospikeException.AsyncQueueFull) {
                blockingQueue.add(proc);
            } else {
                recordListener.onFailure(exception);
            }

        }
    }
}
