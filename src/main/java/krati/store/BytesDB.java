package krati.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import krati.Persistable;
import krati.core.array.SimpleDataArray;
import krati.core.array.basic.DynamicLongArray;
import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;

/**
 * BytesDB.
 * 
 * @author jwu
 *
 */
public final class BytesDB implements Persistable {
    final static Logger _logger = Logger.getLogger(BytesDB.class);
    
    // Main internal objects
    private final SimpleDataArray _dataArray;
    private final DynamicLongArray _addrArray;
    
    // Lookup next index for add methods
    private volatile int _nextIndexCount = 0;
    private final int _nextIndexQueueCapacity = 10000;
    private final LinkedBlockingQueue<Integer> _nextIndexQueue = new LinkedBlockingQueue<Integer>(_nextIndexQueueCapacity);
    private final ExecutorService _nextIndexExecutor = Executors.newSingleThreadExecutor(new LookupThreadFactory());
    
    public BytesDB(File homeDir,
                   int initLevel,
                   int batchSize,
                   int numSyncBatches,
                   int segmentFileSizeMB,
                   SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             batchSize,
             numSyncBatches,
             segmentFileSizeMB,
             segmentFactory,
             0.5);
    }
    
    public BytesDB(File homeDir,
                   int initLevel,
                   int batchSize,
                   int numSyncBatches,
                   int segmentFileSizeMB,
                   SegmentFactory segmentFactory,
                   double segmentCompactFactor) throws Exception {
        _logger.info("init " + homeDir.getAbsolutePath());
        
        // Create address array
        _addrArray = createAddressArray(batchSize, numSyncBatches, homeDir);
        if(initLevel > 0) {
            _addrArray.expandCapacity(_addrArray.subArrayLength() * (1 << initLevel) - 1); 
        }
        
        // Create segment manager
        String segmentHomePath = new File(homeDir, "segs").getAbsolutePath();
        SegmentManager segManager = SegmentManager.getInstance(segmentHomePath, segmentFactory, segmentFileSizeMB);
        
        // Create simple data array
        this._dataArray = new SimpleDataArray(_addrArray, segManager, segmentCompactFactor);
        
        // Scan to count nextIndex
        this.initNextIndexCount();
        
        // Start to lookup nextIndex
        this._nextIndexExecutor.execute(new NextIndexLookup());
        
        _logger.info("init done");
    }
    
    protected DynamicLongArray createAddressArray(int batchSize,
                                                  int numSyncBatches,
                                                  File homeDirectory) throws Exception {
        return new DynamicLongArray(batchSize, numSyncBatches, homeDirectory);
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
    
    public synchronized void clear() {
        _dataArray.clear();
    }
    
    public synchronized void close() throws IOException {
        _dataArray.sync();
        _nextIndexExecutor.shutdown();
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
    public final long getHWMark() {
        return _dataArray.getHWMark();
    }
    
    @Override
    public final long getLWMark() {
        return _dataArray.getLWMark();
    }
    
    @Override
    public final void saveHWMark(long endOfPeriod) throws Exception {
        _dataArray.saveHWMark(endOfPeriod);
    }
    
    private class NextIndexLookup implements Runnable {
        @Override
        public void run() {
            int index = 0;
            int lastPut = -1;
            
            while(true) {
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
                    
                    /* If nextPossible is equal to lastPut, this means lastPust was just dequeued
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
    
    private static class LookupThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    }
}
