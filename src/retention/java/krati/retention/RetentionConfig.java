package krati.retention;

import java.io.File;

import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.io.Serializer;
import krati.retention.clock.Clock;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;
import krati.store.factory.ObjectStoreFactory;

/**
 * RetentionConfig
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/12, 2011 - Created
 */
public class RetentionConfig<T> {
    /**
     * Retention Id.
     */
    private int _id;                                                 // required
    
    /**
     * Retention home directory.
     */
    private File _homeDir;                                           // required
    
    /**
     * Event batch size.
     */
    private int _batchSize = EventBatch.DEFAULT_BATCH_SIZE;
    
    /**
     * Number of event batches needed to sync updates to <tt>indexes.dat</tt>.
     */
    private int _numSyncBatchs = 10;
    
    /**
     * Snapshot store initial size.
     */
    private int _snapshotInitialSize = 10000000;
    
    /**
     * Snapshot store segmentFileSizeMB.
     */
    private int _snapshotSegmentFileSizeMB = 32;
    
    /**
     * Retention store initial size.
     */
    private int _retentionInitialSize = 10000;
    
    /**
     * Retention store segmentFileSizeMB.
     */
    private int _retentionSegmentFileSizeMB = 32;
    
    /**
     * Snapshot segment factory.
     */
    private SegmentFactory _snapshotSegmentFactory = new WriteBufferSegmentFactory();
    
    /**
     * Retention segment factory.
     */
    private SegmentFactory _retentionSegmentFactory = new WriteBufferSegmentFactory();
    
    /**
     * Retention policy.
     */
    private RetentionPolicy _retentionPolicy = new RetentionPolicyOnSize(1000);
    
    /**
     * {@link krati.retention.Event Event} value serializer.
     */
    private Serializer<T> _eventValueSerializer;                     // required

    /**
     * {@link krati.retention.Event Event} clock serializer.
     */
    private Serializer<Clock> _eventClockSerializer;                 // required
    
    /**
     * Snapshot clock store factory.
     */
    private ObjectStoreFactory<T, Clock> _snapshotClockStoreFactory; // required
    
    private final static int SNAPSHOT_INITIAL_SIZE_MIN = 1000;
    private final static int RETENTION_INITIAL_SIZE_MIN = 1000;
    
    public RetentionConfig(int id, File homeDir) {
        this._id = id;
        this._homeDir = homeDir;
    }
    
    public int getId() {
        return _id;
    }
    
    public File getHomeDir() {
        return _homeDir;
    }
    
    public void setSnapshotInitialSize(int snapshotInitialSize) {
        this._snapshotInitialSize = Math.max(SNAPSHOT_INITIAL_SIZE_MIN, snapshotInitialSize);
    }
    
    public int getSnapshotInitialSize() {
        return _snapshotInitialSize;
    }
    
    public void setSnapshotSegmentFileSizeMB(int snapshotSegmentFileSizeMB) {
        this._snapshotSegmentFileSizeMB = Math.max(Segment.minSegmentFileSizeMB, snapshotSegmentFileSizeMB);
    }
    
    public int getSnapshotSegmentFileSizeMB() {
        return _snapshotSegmentFileSizeMB;
    }
    
    public void setRetentionInitialSize(int retentionInitialSize) {
        this._retentionInitialSize = Math.max(RETENTION_INITIAL_SIZE_MIN, retentionInitialSize);
    }
    
    public int getRetentionInitialSize() {
        return _retentionInitialSize;
    }
    
    public void setRetentionSegmentFileSizeMB(int retentionSegmentFileSizeMB) {
        this._retentionSegmentFileSizeMB = Math.max(Segment.minSegmentFileSizeMB, retentionSegmentFileSizeMB);
    }
    
    public int getRetentionSegmentFileSizeMB() {
        return _retentionSegmentFileSizeMB;
    }
    
    public void setSnapshotSegmentFactory(SegmentFactory snapshotSegmentFactory) {
        this._snapshotSegmentFactory = snapshotSegmentFactory == null ?
                new WriteBufferSegmentFactory() : snapshotSegmentFactory;
    }
    
    public SegmentFactory getSnapshotSegmentFactory() {
        return _snapshotSegmentFactory;
    }
    
    public void setRetentionSegmentFactory(SegmentFactory retentionSegmentFactory) {
        this._retentionSegmentFactory = retentionSegmentFactory == null ?
                new WriteBufferSegmentFactory() : retentionSegmentFactory;
    }
    
    public SegmentFactory getRetentionSegmentFactory() {
        return _retentionSegmentFactory;
    }
    
    public void setBatchSize(int batchSize) {
        this._batchSize = Math.max(EventBatch.MINIMUM_BATCH_SIZE, batchSize);
    }
    
    public int getBatchSize() {
        return _batchSize;
    }
    
    public void setNumSyncBatchs(int numSyncBatchs) {
        this._numSyncBatchs = numSyncBatchs;
    }
    
    public int getNumSyncBatchs() {
        return _numSyncBatchs;
    }
    
    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        this._retentionPolicy = retentionPolicy;
    }
    
    public RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }
    
    public void setEventValueSerializer(Serializer<T> eventValueSerializer) {
        this._eventValueSerializer = eventValueSerializer;
    }
    
    public Serializer<T> getEventValueSerializer() {
        return _eventValueSerializer;
    }
    
    public void setEventClockSerializer(Serializer<Clock> eventClockSerializer) {
        this._eventClockSerializer = eventClockSerializer;
    }
    
    public Serializer<Clock> getEventClockSerializer() {
        return _eventClockSerializer;
    }
    
    public void setSnapshotClockStoreFactory(ObjectStoreFactory<T, Clock> clockStoreFactory) {
        this._snapshotClockStoreFactory = clockStoreFactory;
    }
    
    public ObjectStoreFactory<T, Clock> getSnapshotClockStoreFactory() {
        return _snapshotClockStoreFactory;
    }
}
