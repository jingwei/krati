/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
import krati.store.ObjectStore;
import krati.store.factory.ObjectStoreFactory;
import krati.util.IndexedIterator;

/**
 * SimpleSnapshot
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/10, 2011 - Created <br/>
 * 10/11, 2011 - Added clockStoreFactory to constructor <br/>
 * 11/16, 2011 - Put a limit on the number of events upon each call to method get <br/>
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
            ObjectStoreFactory<T, Clock> clockStoreFactory,
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
        _clockStore = clockStoreFactory.create(_clockStoreConfig, eventValueSerializer, eventClockSerializer);
        
        init();
        _logger.info("started");
    }
    
    protected void init() {}
    
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
    public int getBatchSize() {
        return _eventBatchSize;
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
        
        try {
            iter.reset(index);
        } catch(ArrayIndexOutOfBoundsException e) {
            return new SimplePosition(_id, pos.getOffset(), pos.getClock());
        }
        
        Clock evtClock;
        Clock posClock = pos.getClock();
        
        int cnt = 0;
        int lastIndex = index;
        while(iter.hasNext()) {
            lastIndex = iter.index();
            Entry<T, Clock> e = iter.next();
            
            evtClock = e.getValue();
            if(posClock.beforeEqual(evtClock)) {
                list.add(new SimpleEvent<T>(e.getKey(), evtClock));
                cnt++;
            }
            
            if(cnt >= _eventBatchSize) {
                index = iter.index();
                if(lastIndex == index) {
                    while(iter.hasNext() && iter.index() == index) {
                        e = iter.next();
                        
                        evtClock = e.getValue();
                        if(posClock.beforeEqual(evtClock)) {
                            list.add(new SimpleEvent<T>(e.getKey(), evtClock));
                            cnt++;
                        }
                    }
                    index++;
                }
                
                // Exit loop when enough events are collected
                break;
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
    
    @Override
    public void flush() throws IOException {
        _clockStore.persist();
    }
}
