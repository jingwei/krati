package test.retention;

import krati.retention.Retention;
import krati.retention.RetentionConfig;
import krati.retention.SimpleEventBus;
import krati.retention.clock.Clock;
import krati.store.factory.DynamicObjectStoreFactory;

/**
 * TestRetentionStoreReader2
 * 
 * @author jwu
 * @since 11/20, 2011
 */
public class TestRetentionStoreReader2 extends TestRetentionStoreReader {
    
    @Override
    protected Retention<String> createRetention() throws Exception {
        RetentionConfig<String> config = new RetentionConfig<String>(getId(), getHomeDir());
        config.setBatchSize(getEventBatchSize());
        config.setRetentionPolicy(createRetentionPolicy());
        config.setEventValueSerializer(createEventValueSerializer());
        config.setEventClockSerializer(createEventClockSerializer());
        config.setSnapshotClockStoreFactory(new DynamicObjectStoreFactory<String, Clock>());
        config.setRetentionSegmentFileSizeMB(16);
        config.setSnapshotSegmentFileSizeMB(16);
        
        return new SimpleEventBus<String>(config);
    }
}
