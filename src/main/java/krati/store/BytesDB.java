package krati.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import krati.Mode;
import krati.Persistable;
import krati.core.StoreConfig;
import krati.core.array.AddressArray;
import krati.core.array.AddressArrayFactory;
import krati.core.array.SimpleDataArray;
import krati.core.segment.Segment;
import krati.core.segment.SegmentManager;
import krati.io.Closeable;
import krati.util.DaemonThreadFactory;

/**
 * BytesDB.
 * 
 * @author jwu
 * 
 * <p>
 * 05/31, 2011 - Added support for Closeable
 * 06/28, 2011 - Added constructor using StoreConfig
 */
public final class BytesDB implements Persistable, Closeable {
    final static Logger _logger = Logger.getLogger(BytesDB.class);
    
    // Main internal objects
    private final SimpleDataArray _dataArray;
    private final AddressArray _addrArray;
    private final StoreConfig _config;
    
    /**
     * The mode can only be <code>Mode.INIT</code>, <code>Mode.OPEN</code> and <code>Mode.CLOSED</code>.
     */
    private volatile Mode _mode = Mode.INIT;
    
    // Lookup next index for add methods
    private volatile int _nextIndexCount = 0;
    private final int _nextIndexQueueCapacity = 10000;
    private final NextIndexLookup _nextIndexLookup = new NextIndexLookup();
    private final LinkedBlockingQueue<Integer> _nextIndexQueue = new LinkedBlockingQueue<Integer>(_nextIndexQueueCapacity);
    private ExecutorService _nextIndexExecutor = null;
    
    public BytesDB(StoreConfig config) throws Exception {
        config.validate();
        config.save();
        
        // Set StoreConfig
        this._config = config;
        
        // Create address array
        this._addrArray = createAddressArray(
                _config.getInitialCapacity(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.isIndexesCached());
        
        // Create segment manager
        String segmentHomePath = new File(_config.getHomeDir(), "segs").getAbsolutePath();
        SegmentManager segManager = SegmentManager.getInstance(
                segmentHomePath,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        // Create simple data array
        this._dataArray = new SimpleDataArray(_addrArray, segManager, _config.getSegmentCompactFactor());
        
        // Scan to count nextIndex
        this.initNextIndexCount();
        
        // Start to lookup nextIndex
        this._nextIndexLookup.setEnabled(true);
        this._nextIndexExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
        this._nextIndexExecutor.execute(_nextIndexLookup);
        
        // Initialize mode
        this._mode = Mode.OPEN;
        _logger.info("mode=" + _mode);
    }
    
    private AddressArray createAddressArray(int length,
                                            int batchSize,
                                            int numSyncBatches,
                                            boolean indexesCached) throws Exception {
        AddressArrayFactory factory = new AddressArrayFactory(indexesCached);
        AddressArray addrArray = factory.createDynamicAddressArray(getHomeDir(), batchSize, numSyncBatches);
        addrArray.expandCapacity(length - 1);
        return addrArray;
    }
    
    public final File getHomeDir() {
        return _config.getHomeDir();
    }
    
    public final int capacity() {
        return _addrArray.length();
    }
    
    public boolean hasData(int index) {
        return _dataArray.hasData(index);
    }
    
    public boolean hasIndex(int index) {
        return _dataArray.hasIndex(index);
    }
    
    public int getLength(int index) {
        return _dataArray.getLength(index);
    }
    
    public byte[] get(int index) {
        return _dataArray.get(index);
    }
    
    public int get(int index, byte[] data) {
        return _dataArray.get(index, data);
    }
    
    public int get(int index, byte[] data, int offset) {
        return _dataArray.get(index, data, offset);
    }
    
    public synchronized void set(int index, byte[] data, long scn) throws Exception {
        _dataArray.set(index, data, scn);
        if(data == null) _nextIndexCount++;
    }
    
    public synchronized void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        _dataArray.set(index, data, offset, length, scn);
        if(data == null) _nextIndexCount++;
    }
    
    public synchronized int add(byte[] data, long scn) throws Exception {
        int index = _nextIndexQueue.take();
        _dataArray.set(index, data, scn);
        _nextIndexCount--;
        return index;
    }
    
    public synchronized int add(byte[] data, int offset, int length, long scn) throws Exception {
        int index = _nextIndexQueue.take();
        _dataArray.set(index, data, offset, length, scn);
        _nextIndexCount--;
        return index;
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _dataArray.sync();
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public synchronized final void saveHWMark(long endOfPeriod) throws Exception {
        _dataArray.saveHWMark(endOfPeriod);
    }
    
    @Override
    public final long getHWMark() {
        return _dataArray.getHWMark();
    }
    
    @Override
    public final long getLWMark() {
        return _dataArray.getLWMark();
    }
    
    class NextIndexLookup implements Runnable {
        volatile boolean _enabled = true;
        
        @Override
        public void run() {
            int index = 0;
            int lastPut = -1;
            
            while(_enabled) {
                if(index < _addrArray.length()) {
                    long addr = _addrArray.get(index);
                    if(addr < Segment.dataStartPosition) {
                        try {
                            _nextIndexQueue.put(index);
                            lastPut = index;
                        } catch (InterruptedException e) {
                            _logger.warn("Failed to add to _nextIndexQueue", e);
                        }
                    }
                    index++;
                } else {
                    /* Expand address array only if the total count of nextIndexes
                     * is less than 10% of address array capacity.
                     */
                    int threshold = (int)(_addrArray.length() * 0.1);
                    if(_nextIndexCount < threshold) {
                        try {
                            _addrArray.expandCapacity(_addrArray.length());
                            
                            // Try to fill up the remaining capacity
                            int cnt = _nextIndexQueue.remainingCapacity();
                            for(int i = 0; i < cnt; i++) {
                                if(index < _addrArray.length()) {
                                    long addr = _addrArray.get(index);
                                    if(addr < Segment.dataStartPosition) {
                                        try {
                                            _nextIndexQueue.put(index);
                                            lastPut = index;
                                        } catch (InterruptedException e) {
                                            _logger.warn("Failed to add to _nextIndexQueue", e);
                                        }
                                    }
                                    index++;
                                }
                            }
                        } catch (Exception e) {
                            _logger.error("failed to expand _addrArray", e);
                        }
                    }
                    
                    // Start scan from index 0 to find the next possible index  
                    int nextPossible = 0;
                    while(!_nextIndexQueue.isEmpty()) {
                        if(nextPossible < _addrArray.length()) {
                            long addr = _addrArray.get(nextPossible);
                            if(addr < Segment.dataStartPosition) {
                                // Sleep 100 nanoseconds.
                                try {
                                    Thread.sleep(0, 100);
                                } catch (InterruptedException e) {}
                            } else {
                                nextPossible++;
                            }
                        }
                    }
                    
                    /* If nextPossible is equal to lastPut, this means lastPut was just dequeued
                     * by the add methods and it cannot be enqueued into the _nextIndexQueue again.
                     * Need to look for a new nextIndex by incrementing nextPossible.
                     */
                    if(nextPossible == lastPut) {
                        nextPossible++;
                    }
                    index = nextPossible;
                }
            } /* End of while {} */
        }
        
        public void setEnabled(boolean b) {
            this._enabled = b;
        }
        
        public boolean isEnabled() {
            return _enabled;
        }
    }
    
    private void initNextIndexCount() {
        int index = 0;
        int length = _addrArray.length();
        while(index < length) {
            long addr = _addrArray.get(index);
            if(addr < Segment.dataStartPosition) {
                _nextIndexCount++;
            }
            index++;
        }
        
        _logger.info("load " + (length - _nextIndexCount) + "/" + length);
    }
    
    /**
     * Clears all data stored in this BytesDB.
     * This method is not effective if this BytesDB is not open.
     */
    public synchronized void clear() {
        if(isOpen()) {
            _dataArray.clear();
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(_mode == Mode.CLOSED) {
            return;
        }
        
        try {
            // Close dataArray
            _dataArray.sync();
            _dataArray.close();
            
            // Shutdown nextIndex lookup executor
            if(_nextIndexExecutor != null && !_nextIndexExecutor.isShutdown()) {
                _nextIndexLookup.setEnabled(false);
                _nextIndexExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                _nextIndexExecutor.shutdown();
            }
        } catch(Exception e) {
            throw (e instanceof IOException) ?
                  (IOException)e : new IOException("Failed to close", e);
        } finally {
            _mode = Mode.CLOSED;
            _nextIndexCount = 0;
            _nextIndexQueue.clear();
            _nextIndexLookup.setEnabled(false);
            
            _logger.info("mode=" + _mode);
        }
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(_mode == Mode.OPEN) {
            return;
        }
        
        try {
            _dataArray.open();
            
            // Scan to count nextIndex
            initNextIndexCount();
            
            // Start nextIndex lookup executor
            _nextIndexLookup.setEnabled(true);
            _nextIndexExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
            _nextIndexExecutor.execute(_nextIndexLookup);
            
            _mode = Mode.OPEN;
        } catch(Exception e) {
            _mode = Mode.CLOSED;
            
            _nextIndexCount = 0;
            _nextIndexQueue.clear();
            _nextIndexLookup.setEnabled(false);
            
            // Close dataArray if open
            if (_dataArray.isOpen()) {
                _dataArray.close();
            }
            
            // Shutdown nextIndex lookup executor
            if(_nextIndexExecutor != null && !_nextIndexExecutor.isShutdown()) {
                _nextIndexExecutor.shutdown();
            }
            
            throw (e instanceof IOException) ?
                  (IOException)e : new IOException("Failed to close", e);
        } finally {
            _logger.info("mode=" + _mode);
        }
    }
    
    @Override
    public final boolean isOpen() {
        return _mode == Mode.OPEN;
    }
}
