package krati.retention;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.core.StoreConfig;
import krati.core.segment.SegmentFactory;
import krati.io.Serializer;
import krati.retention.clock.Clock;
import krati.retention.policy.RetentionPolicy;
import krati.store.DataStore;
import krati.store.DynamicDataStore;
import krati.store.ObjectStore;
import krati.store.SerializableObjectStore;
import krati.util.IndexedIterator;

/**
 * SimpleSnapshot
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/10, 2011 - Created
 */
class SimpleSnapshot<T> implements Retention<T>, RetentionFlushListener {
    private final static Logger _logger = Logger.getLogger(SimpleSnapshot.class);
    
    protected final int _id;
    protected final int _eventBatchSize;
    protected final StoreConfig _clockStoreConfig;
    protected final ObjectStore<T, Clock> _clockStore;
    protected volatile Clock _maxClock;
    
    public SimpleSnapshot(
            int id, File homeDir,
            int initialCapacity, int batchSize, int numSyncBatches,
            SegmentFactory storeSegmentFactory, int storeSegmentFileSizeMB,
            Serializer<T> eventValueSerializer, Serializer<Clock> eventClockSerializer) throws Exception {
        this._id = id;
        this._eventBatchSize = batchSize;
        
        // Create config
        _clockStoreConfig = new StoreConfig(homeDir, initialCapacity);
        _clockStoreConfig.setBatchSize(batchSize);
        _clockStoreConfig.setNumSyncBatches(numSyncBatches);
        _clockStoreConfig.setSegmentFactory(storeSegmentFactory);
        _clockStoreConfig.setSegmentFileSizeMB(storeSegmentFileSizeMB);
        
        // Create clock store
        _clockStore = new SerializableObjectStore<T, Clock>(
        createDataStore(), eventValueSerializer, eventClockSerializer);
        
        init();
        _logger.info("started");
    }
    
    protected void init() {}
    
    protected DataStore<byte[], byte[]> createDataStore() throws Exception {
        return new DynamicDataStore(_clockStoreConfig);
    }
    
    protected Logger getLogger() {
        return _logger;
    }
    
    @Override
    public int getId() {
        return _id;
    }
    
    @Override
    public long getOrigin() {
        return 0;
    }
    
    @Override
    public long getOffset() {
        return _clockStore.capacity();
    }
    
    @Override
    public Clock getMinClock() {
        return Clock.ZERO;
    }
    
    @Override
    public Clock getMaxClock() {
        return _maxClock;
    }
    
    @Override
    public Clock getClock(long offset) {
        return null;
    }
    
    @Override
    public RetentionPolicy getRetentionPolicy() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Position getPosition(Clock sinceClock) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Position get(Position pos, List<Event<T>> list) {
        if(pos == null || !pos.isIndexed() || pos.getId() != _id) {
            return pos;
        }
        
        int index = pos.getIndex();
        IndexedIterator<Entry<T, Clock>> iter = _clockStore.iterator();
        iter.reset(index);
        
        Clock evtClock;
        Clock posClock = pos.getClock();
        
        int cnt = 0;
        while(iter.hasNext()) {
            Entry<T, Clock> e = iter.next();
            
            evtClock = e.getValue();
            if(posClock.beforeEqual(evtClock)) {
                list.add(new SimpleEvent<T>(e.getKey(), evtClock));
                cnt++;
            }
            
            if(cnt == _eventBatchSize) {
                index = iter.index();
                while(iter.hasNext() && iter.index() == index) {
                    e = iter.next();
                    
                    evtClock = e.getValue();
                    if(posClock.beforeEqual(evtClock)) {
                        list.add(new SimpleEvent<T>(e.getKey(), evtClock));
                        cnt++;
                    }
                }
            }
        }
        
        if(cnt > 0) {
            getLogger().info("Read[" + pos.getIndex() + "," + index + ") " + cnt);
        }
        
        return iter.hasNext() ?
                    new SimplePosition(_id, pos.getOffset(), index, pos.getClock()) :
                    new SimplePosition(_id, pos.getOffset(), pos.getClock());
    }
    
    @Override
    public synchronized boolean put(Event<T> event) throws Exception {
        if(_clockStore.put(event.getValue(), event.getClock())) {
            _maxClock = event.getClock();
            return true;
        }
        return false;
    }
    
    @Override
    public void beforeFlush(EventBatch<?> batch) throws IOException {
        _clockStore.persist();
    }
    
    @Override
    public void afterFlush(EventBatch<?> batch) throws IOException {}
    
    @Override
    public boolean isOpen() {
        return _clockStore.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(!_clockStore.isOpen()) {
            _clockStore.open();
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(_clockStore.isOpen()) {
            _clockStore.close();
        }
    }
}
