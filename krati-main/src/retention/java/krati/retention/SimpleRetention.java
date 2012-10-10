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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import krati.core.StoreConfig;
import krati.core.segment.SegmentFactory;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.retention.clock.Clock;
import krati.retention.clock.Occurred;
import krati.retention.policy.RetentionPolicy;
import krati.store.BytesDB;
import krati.util.DaemonThreadFactory;

/**
 * SimpleRetention
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/28, 2011 - Created <br/>
 * 11/20, 2011 - Added a new constructor based on RetentionConfig <br/>
 * 01/25, 2012 - Fixed switching from bootstrap scan to real-time syncUp <br/>
 * 02/09, 2012 - Added batch merge during flush <br/>
 * 04/19, 2012 - Constructor refactoring <br/>
 */
public class SimpleRetention<T> implements Retention<T> {
    private final static Logger _logger = Logger.getLogger(SimpleRetention.class);
    
    private final int _id;
    private final File _homeDir;
    private final BytesDB _store;
    private final int _eventBatchSize;
    private final EventBatchSerializer<T> _eventBatchSerializer;
    private final ConcurrentLinkedQueue<EventBatchCursor> _retentionQueue = new ConcurrentLinkedQueue<EventBatchCursor>();
    
    private final RetentionPolicy _retentionPolicy;
    private final RetentionPolicyApply _retentionPolicyApply = new RetentionPolicyApply(); 
    private final ScheduledExecutorService _retentionPolicyExecutor = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
    
    /**
     * The current batch, to which new events will be added.
     */
    private volatile EventBatch<T> _batch = null;
    
    /**
     * The last batch persisted to disk.
     */
    private volatile EventBatch<T> _lastBatch = null;
    
    /**
     * The last batch cursor, which is valid if and only if the last batch is not null. 
     */
    private volatile EventBatchCursor _lastBatchCursor = null;
    
    /**
     * The lock for protecting the assignment of <code>_batch</code> to <code>_lastBatch</code>.
     */
    private final Lock _batchLock = new ReentrantLock();
    
    /**
     * The listener which is interested in the current batch is flushed for persistency.
     */
    private RetentionFlushListener _flushListener = null;
    
    /**
     * Constructs a new instance of SimpleRetention
     * 
     * @param config - the retention configuration
     * @throws Exception if this retention cannot be created or instantiated for any reasons.
     */
    public SimpleRetention(RetentionConfig<T> config) throws Exception {
        this(config.getId(),
             new File(config.getHomeDir(), "retention"),
             config.getRetentionInitialSize(),
             config.getRetentionPolicy(),
             new SimpleEventBatchSerializer<T>(
                     config.getEventValueSerializer(),
                     config.getEventClockSerializer()),
             config.getBatchSize(),
             config.getNumSyncBatchs(),
             config.getRetentionSegmentFactory(),
             config.getRetentionSegmentFileSizeMB());
    }
    
    /**
     * Constructs a new instance of SimpleRetention.
     * <p>
     * The constructed instance has the initial capacity of 10000 event batches
     * and synchronizes changes every 10 event batches. The default segment factory
     * is {@link WriteBufferSegmentFactory}, which produces {@link krati.core.segment.Segment Segment} of 32MB.
     * </p>
     * 
     * @param id                - the retention Id
     * @param homeDir           - the retention home directory
     * @param retentionPolicy   - the retention policy for purging event batches
     * @param batchSerializer   - the serializer of {@link EventBatch}
     * @param batchSize         - the size of {@link EventBatch} (number of events)
     * @throws Exception if this retention cannot be created or instantiated for any reasons.
     */
    public SimpleRetention(int id, File homeDir,
                           RetentionPolicy retentionPolicy,
                           EventBatchSerializer<T> batchSerializer, int batchSize) throws Exception {
        this(id, homeDir, 100000,
             retentionPolicy, batchSerializer, batchSize,
             new WriteBufferSegmentFactory(), 32 /* storeSegmentFileSizeMB */);
    }
    
    /**
     * Constructs a new instance of SimpleRetention.
     * <p>
     * The constructed instance synchronizes changes every 10 event batches.
     * </p>
     * 
     * @param id                - the retention Id
     * @param homeDir           - the retention home directory
     * @param initialSize       - the retention initial size (number of event batches)
     * @param retentionPolicy   - the retention policy for purging event batches
     * @param batchSerializer   - the serializer of {@link EventBatch}
     * @param batchSize         - the size of {@link EventBatch} (number of events)
     * @param segmentFactory    - the underlying store segment factory
     * @param segmentFileSizeMB - the underlying store segment file size in MB
     * @throws Exception if this retention cannot be created or instantiated for any reasons.
     */
    public SimpleRetention(int id,
                           File homeDir, int initialSize,
                           RetentionPolicy retentionPolicy,
                           EventBatchSerializer<T> batchSerializer, int batchSize,
                           SegmentFactory segmentFactory, int segmentFileSizeMB) throws Exception {
        this(id, homeDir, initialSize,
             retentionPolicy, batchSerializer,
             batchSize, 10 /* numSyncBatches */,
             segmentFactory, segmentFileSizeMB);
    }
    
    /**
     * Constructs a new instance of SimpleRetention.
     * 
     * @param id                - the retention Id
     * @param homeDir           - the retention home directory
     * @param initialSize       - the retention initial size (number of event batches)
     * @param retentionPolicy   - the retention policy for purging event batches
     * @param batchSerializer   - the serializer of {@link EventBatch}
     * @param batchSize         - the size of {@link EventBatch} (number of events)
     * @param numSyncBatches    - the number of event batches needed to sync changes 
     * @param segmentFactory    - the underlying store segment factory
     * @param segmentFileSizeMB - the underlying store segment file size in MB
     * @throws Exception if this retention cannot be created or instantiated for any reasons.
     */
    protected SimpleRetention(int id,
                              File homeDir, int initialSize,
                              RetentionPolicy retentionPolicy,
                              EventBatchSerializer<T> batchSerializer,
                              int batchSize, int numSyncBatches,
                              SegmentFactory segmentFactory, int segmentFileSizeMB) throws Exception {
        this._id = id;
        this._homeDir = homeDir;
        this._retentionPolicy = retentionPolicy;
        this._eventBatchSerializer = batchSerializer;
        this._eventBatchSize = Math.max(EventBatch.MINIMUM_BATCH_SIZE, batchSize);
        
        StoreConfig config = new StoreConfig(homeDir, initialSize);
        /********************************************************
         * NOTE: 1 is required to flush every update to BytesDB *
         ********************************************************/
        config.setBatchSize(1);
        config.setNumSyncBatches(numSyncBatches);
        config.setSegmentFileSizeMB(segmentFileSizeMB);
        config.setSegmentFactory(segmentFactory);
        _store = new BytesDB(config);
        
        // Initialize
        init();
    }
    
    protected void init() throws IOException {
        _store.sync();
        
        int length = _store.capacity();
        ArrayList<EventBatchCursor> list = new ArrayList<EventBatchCursor>(length / 2);
        for(int index = 0; index < length; index++) {
            if(_store.hasData(index)) {
              try {
                  byte[] bytes = _store.get(index);
                  EventBatchHeader header = _eventBatchSerializer.deserializeHeader(bytes);
                  EventBatchCursor cursor = new SimpleEventBatchCursor(index, header);
                  list.add(cursor);
              } catch(Exception e) {
                  _logger.error("Failed to open a cursor", e);
              }
            }
        }
        
        Clock batchClock = Clock.ZERO;
        long batchOrigin = 0L;
        int cnt = list.size();
        
        if (cnt > 0) {
            Collections.sort(list, new Comparator<EventBatchCursor>() {
                @Override
                public int compare(EventBatchCursor c1, EventBatchCursor c2) {
                    return (int)(c1.getHeader().getOrigin() - c2.getHeader().getOrigin());
                }
            });
            
            for(int i = 0; i < cnt; i++) {
                _retentionQueue.add(list.get(i));
            }
            
            EventBatchHeader header = list.get(cnt - 1).getHeader();
            batchOrigin = header.getOrigin() + header.getSize();
            batchClock = header.getMaxClock();
        }
        
        this._batch = nextEventBatch(batchOrigin, batchClock);
        this._lastBatch = null;
        this._lastBatchCursor = null;
        
        scheduleRetentionPolicy();
        
        // Print initial position
        _logger.info("init " + cnt + " batches");
        _logger.info("init position=" + getPosition());
        _logger.info("init batch=" + _batch);
    }
    
    protected EventBatch<T> nextEventBatch(long offset, Clock initClock) {
        EventBatch<T> b = new SimpleEventBatch<T>(offset, initClock, _eventBatchSize);
        _logger.info("Created EventBatch: " + b.getOrigin());
        return b;
    }
    
    protected void scheduleRetentionPolicy() {
        // Schedule retention policy with a fixed delay of 5 seconds
        _retentionPolicyExecutor.scheduleWithFixedDelay(_retentionPolicyApply, 1, 5, TimeUnit.SECONDS);
    }
    
    public final File getHomeDir() {
        return _homeDir;
    }
    
    public final int getEventBatchSize() {
        return _eventBatchSize;
    }
    
    public final EventBatchSerializer<T> getEventBatchSerializer() {
        return _eventBatchSerializer;
    }
    
    public final RetentionFlushListener getFlushListener() {
        return _flushListener;
    }
    
    public final void setFlushListener(RetentionFlushListener l) {
        this._flushListener = l;
    }
    
    @Override
    public final int getId() {
        return _id;
    }
    
    @Override
    public long getOrigin() {
        long batchOrigin = _batch.getOrigin();
        EventBatchCursor cursor = _retentionQueue.peek();
        long ret = (cursor == null) ? batchOrigin : cursor.getHeader().getOrigin();
        return ret;
    }
    
    @Override
    public long getOffset() {
        return _batch.getOrigin() + _batch.getSize();
    }
    
    @Override
    public Clock getMinClock() {
        Clock batchMinClock = _batch.getMinClock();
        EventBatchCursor cursor = _retentionQueue.peek();
        return cursor == null ? batchMinClock : cursor.getHeader().getMinClock();
    }
    
    @Override
    public Clock getMaxClock() {
        return _batch.getMaxClock();
    }
    
    @Override
    public Clock getClock(long offset) {
        EventBatch<T> b;
        Clock clock;
        
        if(offset < getOrigin()) {
            return null;
        }
        
        if(offset >= getOffset()) {
            return getMaxClock();
        }
        
        _batchLock.lock();
        try {
            // Get position from _batch
            b = _batch;
            clock = b.getClock(offset);
            if(clock != null) return clock;
            
            // Get position from _lastBatch
            b = _lastBatch;
            if(b != null) {
                clock = b.getClock(offset);
                if(clock != null) return clock;
            }
        } finally {
            _batchLock.unlock();
        }
        
        // Get position from the batches in retention
        int cnt = 0;
        Iterator<EventBatchCursor> iter = _retentionQueue.iterator();
        while(iter.hasNext()) {
            EventBatchCursor c = iter.next();
            EventBatchHeader h = c.getHeader();
            long start = h.getOrigin();
            
            if(start <= offset) {
                if(offset < (start + h.getSize())) {
                    byte[] dat = _store.get(c.getLookup());
                    try {
                        b = _eventBatchSerializer.deserialize(dat);
                        clock = b.getClock(offset);
                        if(clock != null) return clock;
                    } catch(Exception e) {
                        _logger.warn(e.getMessage());
                    }
                }
            } else {
                if(cnt == 0) {
                    break;
                }
            }
            cnt++;
        }
        
        return null;
    }
    
    @Override
    public final int getBatchSize() {
        return _eventBatchSize;
    }
    
    @Override
    public final RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }
    
    @Override
    public final Position getPosition() {
        return new SimplePosition(getId(), getOffset(), getMaxClock());
    }
    
    @Override
    public Position getPosition(Clock sinceClock) {
        long sinceOffset;
        
        Occurred occ = sinceClock.compareTo(getMinClock());
        if(occ == Occurred.EQUICONCURRENTLY) {
            return new SimplePosition(getId(), getOrigin(), getMinClock());
        }
        
        if(occ == Occurred.BEFORE || occ == Occurred.CONCURRENTLY) {
            return null;
        }
        
        if(sinceClock.after(getMaxClock())) {
            return getPosition();
        }
        
        _batchLock.lock();
        try {
            // Get position from _batch
            EventBatch<T> b1 = _batch;
            sinceOffset = b1.getOffset(sinceClock);
            if(sinceOffset >= 0) {
                return new SimplePosition(getId(), sinceOffset, b1.getClock(sinceOffset));
            }
            
            // Get position from _lastBatch
            EventBatch<T> b2 = _lastBatch;
            if(b2 != null) {
                if(b2.getMaxClock().before(sinceClock)) {
                    if(b1.getMinClock().compareTo(sinceClock) == Occurred.EQUICONCURRENTLY) {
                        return new SimplePosition(getId(), b1.getOrigin(), b1.getMinClock());
                    } else {
                        sinceOffset = b2.getOrigin() + b2.getSize();
                        return new SimplePosition(getId(), sinceOffset, b2.getClock(sinceOffset));
                    }
                }
                
                sinceOffset = b2.getOffset(sinceClock);
                if(sinceOffset >= 0) {
                    return new SimplePosition(getId(), sinceOffset, b2.getClock(sinceOffset));
                }
            }
        } finally {
            _batchLock.unlock();
        }
        
        // Get position from the batches in retention
        int cnt = 0;
        Iterator<EventBatchCursor> iter = _retentionQueue.iterator();
        while(iter.hasNext()) {
            EventBatchCursor c = iter.next();
            EventBatchHeader h = c.getHeader();
            
            occ = h.getMinClock().compareTo(sinceClock);
            if(occ == Occurred.EQUICONCURRENTLY) {
                if(cnt == 0) {
                    /* Cannot be sure that the earliest position is sufficient
                     * for the given sinceClock. So need to return null instead.
                     */
                    break;
                }
                return new SimplePosition(getId(), h.getOrigin(), h.getMinClock());
            } else if(occ == Occurred.BEFORE) {
                if(!sinceClock.after(h.getMaxClock())) {
                    byte[] dat = _store.get(c.getLookup());
                    try {
                        EventBatch<T> b = _eventBatchSerializer.deserialize(dat);
                        sinceOffset = b.getOffset(sinceClock);
                        if(sinceOffset >= 0) {
                            return new SimplePosition(getId(), sinceOffset, b.getClock(sinceOffset));
                        }
                    } catch(Exception e) {
                        _logger.warn(e.getMessage());
                    }
                }
            } else {
                if(cnt == 0) {
                    break;
                }
            }
            cnt++;
        }
        
        return null;
    }
    
    /**
     * Gets a number of events starting from a given position in the Retention.
     * The number of events is determined internally by the Retention and it is
     * up to the batch size.   
     * 
     * @param pos  - the retention position from where events will be read
     * @param list - the event list to fill in
     * @return The next position from where new events will be read from the Retention.
     *         If the <tt>pos</tt> occurs before the origin of the Retention or is in the
     *         indexed form, the value <tt>null</tt> is returned.
     */
    @Override
    public Position get(Position pos, List<Event<T>> list) {
        EventBatch<T> b;
        
        // Return null if the position is out of retention or in the indexed form.
        if(pos.getOffset() < getOrigin() || pos.isIndexed()) {
            return null;
        }
        
        // Get events from _batch        
        b = _batch;
        if(b.getOrigin() <= pos.getOffset()) {
            long newOffset = b.get(pos.getOffset(), list);
            Clock clock = pos.getOffset() < newOffset ?
                    b.getClock(newOffset - 1) : pos.getClock(); 
            return new SimplePosition(getId(), newOffset, clock);
        }
        
        // Get events from _lastBatch
        b = _lastBatch;
        if(b != null && b.getOrigin() <= pos.getOffset()) {
            long newOffset = b.get(pos.getOffset(), list);
            Clock clock = pos.getOffset() < newOffset ?
                    b.getClock(newOffset - 1) : pos.getClock();
            return new SimplePosition(getId(), newOffset, clock);
        }
        
        // Get events from batches in retention
        int cnt = 0;
        Iterator<EventBatchCursor> iter = _retentionQueue.iterator();
        while(iter.hasNext()) {
            EventBatchCursor c = iter.next();
            if(c.getHeader().getOrigin() <= pos.getOffset()) {
                byte[] dat = _store.get(c.getLookup());
                try {
                    b = _eventBatchSerializer.deserialize(dat);
                    long newOffset = b.get(pos.getOffset(), list);
                    if(pos.getOffset() < newOffset) {
                        Clock clock = b.getClock(newOffset - 1);
                        return new SimplePosition(getId(), newOffset, clock);
                    }
                } catch(Exception e) {
                    _logger.warn("Ignored EventBatch: " + c.getHeader().getOrigin());
                }
            } else {
                // early stop
                if(cnt == 0) {
                    break;
                }
            }
            cnt++;
        }
        
        return null;
    }
    
    @Override
    public synchronized boolean put(Event<T> event) throws Exception {
        if(_batch.isFull()) {
            _batch.setCompletionTime(System.currentTimeMillis());
            byte[] bytes = _eventBatchSerializer.serialize(_batch);
            
            if(_flushListener != null) {
                _flushListener.beforeFlush(_batch);
            }
            
            /* Flush starts automatically upon adding _batch to BytesDB
             * because the constructor sets update batchSize to 1.
             */
            int batchId = _store.add(bytes, getOffset());
            
            if(_flushListener != null) {
                _flushListener.afterFlush(_batch);
            }
            
            // Add current batch to cursor queue
            EventBatchCursor cursor = new SimpleEventBatchCursor(batchId, _batch.getHeader());
            _retentionQueue.offer(cursor);
            
            // Lock when assign _batch to _lastBatch
            _batchLock.lock();
            try {
                // Reset the lastBatch
                _lastBatch = _batch;
                _lastBatchCursor = cursor;
                
                // Create the next batch
                _batch = nextEventBatch(_batch.getOrigin() + _batch.getSize(), event.getClock());
            } finally {
                _batchLock.unlock();
            }
        }
        
        return _batch.put(event);
    }
    
    private class RetentionPolicyApply implements Runnable {
        @Override
        public void run() {
            Collection<EventBatchCursor> discard = _retentionPolicy.apply(_retentionQueue);
            if(discard != null && discard.size() > 0) {
                for(EventBatchCursor c : discard) {
                    int index = c.getLookup();
                    try {
                        // Apply callback
                        if(_retentionPolicy.isCallback()) {
                            try {
                                byte[] dat = _store.get(index);
                                EventBatch<T> b = _eventBatchSerializer.deserialize(dat);
                                _retentionPolicy.applyCallbackOn(b);
                            } catch(Exception e) {
                                if(_store.isOpen()) {
                                    _logger.error("Failed to apply callback on cursor: " + c.getHeader().getOrigin(), e);
                                }
                            }
                        }
                        
                        // Remove batch permanently
                        _store.set(index, null, getOffset());
                        _logger.info("Removed EventBatch: " + c.getHeader().getOrigin());
                    } catch(Exception e) {
                        if(_store.isOpen()) {
                            _logger.error("Failed to apply retention policy on cursor " + index, e.getCause());
                        }
                    }
                }
            }
        }
    }
    
    @Override
    public boolean isOpen() {
        return _store.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(!_store.isOpen()) {
            _store.open();
            scheduleRetentionPolicy();
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(_store.isOpen()) {
            _retentionPolicyExecutor.shutdown();
            _store.close();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if(isOpen() && !_batch.isEmpty()) {
            // Try to add to the _lastBatch
            if (mergeEventsToLastBatch()) return;
            
            _batch.setCompletionTime(System.currentTimeMillis());
            byte[] bytes = _eventBatchSerializer.serialize(_batch);
            
            if(_flushListener != null) {
                _flushListener.beforeFlush(_batch);
            }
            
            /* Flush starts automatically upon adding _batch to BytesDB
             * because the constructor sets update batchSize to 1.
             */
            int batchId = 0;
            try {
                batchId = _store.add(bytes, getOffset());
            } catch (Exception e) {
                if(e instanceof IOException) {
                    throw (IOException)e;
                } else {
                    throw new IOException(e);
                }
            }
            
            if(_flushListener != null) {
                _flushListener.afterFlush(_batch);
            }
            
            // Add current batch to cursor queue
            EventBatchCursor cursor = new SimpleEventBatchCursor(batchId, _batch.getHeader());
            _retentionQueue.offer(cursor);
            
            // Lock when assigning _batch to _lastBatch
            _batchLock.lock();
            try {
                // Reset the lastBatch
                _lastBatch = _batch;
                _lastBatchCursor = cursor;
                
                // Create the next batch
                _batch = nextEventBatch(_batch.getOrigin() + _batch.getSize(), _batch.getMaxClock());
            } finally {
                _batchLock.unlock();
            }
        }
    }
    
    /**
     * Try to merge events from the current batch to the last persisted batch
     * in order to reduce the number of smaller batches in the retention.
     * 
     * @return <code>true</code> if the merge operation is performed successfully
     * @throws IOException
     */
    protected boolean mergeEventsToLastBatch() throws IOException {
        // Checks if we can merge _batch into _lastBatch
        if(_lastBatch != null && _eventBatchSize >= (_lastBatch.getSize() + _batch.getSize())) {
            _batch.setCompletionTime(System.currentTimeMillis());
            
            if(_flushListener != null) {
                _flushListener.beforeFlush(_batch);
            }
            
            // Creates a copy of the _lastBatch
            SimpleEventBatch<T> copy = ((SimpleEventBatch<T>)_lastBatch).clone();
            
            try {
                // Add events from _batch to the copy
                Iterator<Event<T>> iter = _batch.iterator();
                while(iter.hasNext()) {
                    copy.put(iter.next());
                }
                copy.setCompletionTime(_batch.getCompletionTime());
                
                /* Flush starts automatically upon adding _batch to BytesDB
                 * because the constructor sets update batchSize to 1.
                 */
                byte[] bytes = _eventBatchSerializer.serialize(copy);
                _store.set(_lastBatchCursor.getLookup(), bytes, getOffset());
            } catch (Exception e) {
                _logger.info("events merge aborted", e);
                return false;
            }
            
            if(_flushListener != null) {
                _flushListener.afterFlush(_batch);
            }
            
            _batchLock.lock();
            try {
                // Updated _lastBatch
                _lastBatch = copy;
                _lastBatchCursor.setHeader(copy.getHeader());
                _logger.info(_batch.getSize() + " events merged to EventBatch " + _lastBatchCursor.getLookup());
                
                // Create the next batch
                _batch = nextEventBatch(_batch.getOrigin() + _batch.getSize(), _batch.getMaxClock());
            } finally {
                _batchLock.unlock();
            }
            
            return true;
        }
        
        return false;
    }
}
