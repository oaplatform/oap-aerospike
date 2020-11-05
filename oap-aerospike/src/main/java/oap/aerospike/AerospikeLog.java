package oap.aerospike;

import com.aerospike.client.Log;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by igor.petrenko on 29.05.2019.
 */
@Slf4j
public class AerospikeLog implements Log.Callback {
    public static void init(Log.Level level) {
        Log.setLevel(level);
        Log.setCallback(new AerospikeLog());
    }

    @Override
    public void log(Log.Level level, String message) {
        switch (level) {
            case DEBUG:
                log.trace(message);
                break;
            case INFO:
                log.debug(message);
                break;
            case ERROR:
                log.warn(message);
                break;
            case WARN:
                log.info(message);
                break;
        }
    }
}
