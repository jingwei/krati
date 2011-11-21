package krati.retention;

import java.io.File;
import java.io.IOException;
import java.util.List;

import krati.Mode;
import krati.core.OperationAbortedException;
import krati.retention.clock.Clock;
import krati.retention.policy.RetentionPolicy;

import org.apache.log4j.Logger;

/**
 * SimpleEventBus
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created <br/>
 * 10/07, 2011 - Abort bootstrap if it takes longer than retention time <br/>
 */
public class SimpleEventBus<K> implements Retention<K> {
    private final static Logger _logger = Logger.getLogger(SimpleEventBus.class);
    protected final RetentionConfig<K> _config;
    protected SimpleSnapshot<K> _snapshot;
    protected SimpleRetention<K> _retention;
    private volatile Mode _mode = Mode.OPEN;
    
    /**
     * Creates a new instance of SimpleEventBus.
     * 
     * @param config - configuration
     * @throws Exception
     */
    public SimpleEventBus(RetentionConfig<K> config) throws Exception {
        this._config = config;
        if(_config.getEventValueSerializer() == null) {
            throw new Exception("Invalid configuration: eventValueSerializer not found");
        }
        
        if(_config.getEventClockSerializer() == null) {
            throw new Exception("Invalid configuration: eventClockSerializer not found");
        }
        
        if(_config.getSnapshotClockStoreFactory() == null) {
            throw new Exception("Invalid configuration: clockStoreFactory not found");
        }
        
        _retention = new SimpleRetention<K>(
                _config.getId(),
                new File(_config.getHomeDir(), "retention"),
                _config.getRetentionInitialSize(),
                _config.getRetentionPolicy(),
                new SimpleEventBatchSerializer<K>(
                        _config.getEventValueSerializer(),
                        _config.getEventClockSerializer()),
                _config.getBatchSize(),
                _config.getRetentionSegmentFactory(),
                _config.getRetentionSegmentFileSizeMB());
        
        _snapshot = new SimpleSnapshot<K>(
                _config.getId(),
                new File(_config.getHomeDir(), "snapshot"),
                _config.getSnapshotInitialSize(),
                _config.getBatchSize(),
                _config.getNumSyncBatchs(),
                _config.getSnapshotSegmentFactory(),
                _config.getSnapshotSegmentFileSizeMB(),
                _config.getSnapshotClockStoreFactory(),
                _config.getEventValueSerializer(),
                _config.getEventClockSerializer());
        
        _retention.setFlushListener(_snapshot);
        
        _logger.info("started");
    }
    
    @Override
    public final int getId() {
        return _config.getId();
    }
    
    @Override
    public final long getOffset() {
        return _retention.getOffset();
    }
    
    @Override
    public final long getOrigin() {
        return _retention.getOrigin();
    }
    
    @Override
    public Clock getMinClock() {
        return _retention.getMinClock();
    }
    
    @Override
    public Clock getMaxClock() {
        return _retention.getMaxClock();
    }
    
    @Override
    public Clock getClock(long offset) {
        return _retention.getClock(offset);
    }
    
    @Override
    public final int getBatchSize() {
        return _retention.getBatchSize();
    }
    
    @Override
    public final RetentionPolicy getRetentionPolicy() {
        return _retention.getRetentionPolicy();
    }
    
    @Override
    public final Position getPosition() {
        return _retention.getPosition();
    }
    
    @Override
    public Position getPosition(Clock sinceClock) {
        Position pos;
        
        if(Clock.ZERO != sinceClock) {
            pos = _retention.getPosition(sinceClock);
            if(pos != null) {
                return pos;
            }
        }
        
        return new SimplePosition(getId(), getOffset(), 0, sinceClock);
    }
    
    @Override
    public Position get(Position pos, List<Event<K>> list) {
        if(pos == null || list == null) {
            throw new NullPointerException();
        }
        
        if(pos.getId() != getId()) {
            if(pos.isIndexed()) {
                throw new InvalidPositionException("Bootstrap reconnection rejected", pos);
            } else {
                pos = getPosition(pos.getClock());
            }
        }
        
        // STEP 1: Get from retention 
        if(!pos.isIndexed()) {
            long offset = pos.getOffset();
            
            if(_retention.getOffset() <= offset) {
                return pos;
            }
            
            if(_retention.getOrigin() <= offset) {
                Position nextPos = _retention.get(pos, list);
                if(nextPos != null) {
                    return nextPos;
                }
            }
        }
        
        // STEP 2: Get from snapshot
        if(pos.isIndexed()) {
            // Read from the middle of snapshot
            Position nextPos = _snapshot.get(pos, list);
            if(!nextPos.isIndexed()) {
                Clock clock = getClock(nextPos.getOffset());
                if(clock != null) {
                    /**
                     * Bootstrap is successful without falling out of retention.
                     */
                    nextPos = new SimplePosition(getId(), nextPos.getOffset(), clock);
                    _logger.info("Bootstrap success: Position=" + nextPos);
                } else {
                    /**
                     * Bootstrap takes longer than retention time and need to abort.
                     */
                    nextPos = new SimplePosition(getId(), getOffset(), 0, nextPos.getClock());
                    throw new OperationAbortedException("Bootstrap aborted: Position=" + nextPos);
                }
            }
            return nextPos;
        } else {
            // Read from the beginning of snapshot
            Position boostrapStart = new SimplePosition(getId(), getOffset(), 0, pos.getClock());
            _logger.warn("Bootstrap started: Position=" + boostrapStart);
            return _snapshot.get(boostrapStart, list);
        }
    }
    
    @Override
    public synchronized boolean put(Event<K> event) throws Exception {
        if(event == null || event.getValue() == null | event.getClock() == null) {
            return false;
        }
        
        // Check that event has a monotonically increasing clock.  
        if(!event.getClock().afterEqual(getMaxClock())) {
            return false;
        }
        
        if(_snapshot.put(event)) {
            return _retention.put(event);
        }
        
        return false;
    }
    
    @Override
    public boolean isOpen() {
        return _mode == Mode.OPEN;
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(!isOpen()) {
            try {
                _snapshot.open();
                _retention.open();
                _mode = Mode.OPEN;
            } catch(IOException e) {
                _mode = Mode.CLOSED;
                if(_snapshot.isOpen()) {
                    _snapshot.close();
                }
                throw e;
            }
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(isOpen()) {
            try {
                _snapshot.close();
                _retention.close();
            } catch(IOException e) {
                if(_retention.isOpen()) {
                    _retention.close();
                }
                throw e;
            } finally {
                _mode = Mode.CLOSED;
            }
        }
    }
    
    @Override
    public synchronized void flush() throws IOException {
        if(isOpen()) {
            _snapshot.flush();
            _retention.flush();
        }
    }
}
