/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package krati.core.array;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import krati.core.StoreParams;
import org.apache.log4j.Logger;

import krati.core.segment.AddressFormat;
import krati.core.segment.MemorySegment;
import krati.core.segment.Segment;
import krati.core.segment.SegmentManager;
import krati.util.Chronos;

/**
 * SimpleDataArray Compactor.
 * <p>
 * The compaction is a two-stage process: <strong>inspect</strong> and <strong>compact</strong>.
 * </p>
 * <p>
 * The <strong>inspect</strong> determines which segments are eligible for compaction according to
 * a predefined load factor. It returns no more than 3 source segments for compaction.
 * </p>
 * <p>
 * The <strong>compact</strong> transfers bytes from source segments to a target segment via zero-copy.
 * It batches compaction update records and sends them to the writer for post-processing. 
 * </p>
 * 
 * @author jwu
 * 
 * <p>
 * 05/22, 2011 - Fixed start/shutdown <br/>
 * 05/23, 2011 - Added method clear() to clean up compactor internal state <br/>
 * 06/21, 2011 - Added support for tolerating compaction failure <br/>
 */
class SimpleDataArrayCompactor implements Runnable {
    private final static Logger _log = Logger.getLogger(SimpleDataArrayCompactor.class);
    private ExecutorService _executor = Executors.newSingleThreadExecutor(new CompactorThreadFactory());
    private SimpleDataArray _dataArray;
    
    /**
     * Whether this compactor is enabled.
     */
    private volatile boolean _enabled = true;
    
    /**
     * Compactor shutdown timeout in milliseconds (default 5000).
     */
    private long _shutdownTimeout = 5000;
    
    /**
     * The load factor of segment to determine the legibility of compaction.
     */
    private volatile double _compactLoadFactor;
    
    /**
     * The internal state of compactor during the current running cycle.  
     */
    private volatile State _state = State.DONE;
    
    /**
     * Reclaim segments in _segSourceList and transfer their content to _segTarget.
     */
    private volatile Segment _segTarget;
    private final ArrayList<Segment> _segSourceList;
    
    /**
     * Lock for synchronizing compactor executions.
     */
    private final ReentrantLock _lock = new ReentrantLock();
    
    /**
     * Manage compaction updates that will be consumed by the writer.
     */
    private final CompactionUpdateManager _updateManager;
    
    /**
     * The writer signals the compactor to start a new compaction cycle. 
     */
    private final AtomicBoolean _newCycle = new AtomicBoolean(false);
    
    /**
     * Queue for the compactor to send the writer the target segment as nextSegment. 
     */
    private final ConcurrentLinkedQueue<Segment> _targetQueue =
        new ConcurrentLinkedQueue<Segment>();
    
    /**
     * Queue for segments compacted successfully by the compactor. 
     */
    private final ConcurrentLinkedQueue<Segment> _compactedQueue =
        new ConcurrentLinkedQueue<Segment>();
    
    /**
     * Permits for the writer to get next segment without being blocked.
     */
    private final AtomicInteger _segPermits = new AtomicInteger(0);
    
    /**
     * Segments ignored for compaction due to unknown exceptions.
     */
    private final Set<Segment> _ignoredSegs = Collections.synchronizedSet(new HashSet<Segment>());
    
    /**
     * A byte buffer from transferring bytes to a target segment. 
     */
    private ByteBuffer _buffer = null;
    
    /**
     * Constructs a DataArrayCompactor with the setting below:
     * 
     * <pre>
     *   Compact Load Factor : 0.5
     *   Compact batch Size  : 1000
     * </pre>
     * <p>
     * A segment is eligible for compaction only if its load factor is less than
     * the default compact load factor (0.5).
     * </p>
     * @param dataArray          the data array to compact
     */
    public SimpleDataArrayCompactor(SimpleDataArray dataArray) {
        this(dataArray, StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT, 1000);
    }
    
    /**
     * Constructs a DataArrayCompactor with a specified compact load factor and a default compact batch size (1000).
     * <p>
     * A segment is eligible for compaction only if its load factor is less than
     * the user-specified compact load factor.
     * </p>
     * @param dataArray          the data array to compact
     * @param compactLoadFactor  the load factor below which a segment is eligible for compaction
     */
    public SimpleDataArrayCompactor(SimpleDataArray dataArray, double compactLoadFactor) {
        this(dataArray, compactLoadFactor, 1000);
    }
    
    /**
     * Constructs a DataArrayCompactor with a specified compact load factor and compact batch size.
     * <p>
     * A segment is eligible for compaction only if its load factor is less than
     * the user-specified compact load factor.
     * </p>
     * @param dataArray          the data array to compact
     * @param compactLoadFactor  the load factor below which a segment is eligible for compaction
     * @param compactBatchSize   the size of compaction updates delivered by the compactor to the writer. 
     */
    public SimpleDataArrayCompactor(SimpleDataArray dataArray, double compactLoadFactor, int compactBatchSize) {
        this._dataArray = dataArray;
        this._compactLoadFactor = compactLoadFactor;
        this._segSourceList = new ArrayList<Segment>();
        this._updateManager = new CompactionUpdateManager(_dataArray, compactBatchSize);
    }
    
    /**
     * Gets the load factor below which a segment is eligible for compaction.
     */
    public double getCompactLoadFactor() {
        return this._compactLoadFactor;
    }
    
    /**
     * A comparator for ordering Segments based on the Segment load factor.
     */
    private static Comparator<Segment> _segmentLoadCmp = new Comparator<Segment>() {
        @Override
        public int compare(Segment s1, Segment s2) {
            double load1 = s1.getLoadSize();
            double load2 = s2.getLoadSize();
            return (load1 < load2) ? -1 : ((load1 == load2) ? 0 : 1);
        }
    };
    
    /**
     * Inspects and finds the most fragmented Segments for compaction.
     * <p>
     * The number of Segments found for compaction is not greater than 3.
     * In other words, each compaction cycle reclaims no more than 3 Segments. 
     * </p>
     * 
     * @return <code>true</code> is at least one Segment is found to be eligible for compaction.
     */
    private boolean inspect() {
        SegmentManager segManager = _dataArray.getSegmentManager();
        if(segManager == null) return false;
        
        synchronized(segManager) {
            Segment segCurrent = _dataArray.getCurrentSegment();
            
            /*
             * Find source segments that are least loaded.
             * The source segments must be in the READ_ONLY mode.
             */
            ArrayList<Segment> recycleList = new ArrayList<Segment>();
            int cnt = segManager.getSegmentCount();
            for(int i = 0; i < cnt; i++) {
                Segment seg = segManager.getSegment(i);
                if(seg != null && seg.getMode() == Segment.Mode.READ_ONLY && seg != segCurrent) {
                    if (seg.getLoadFactor() < _compactLoadFactor && !_ignoredSegs.contains(seg)) {
                        recycleList.add(seg);
                    }
                }
            }
            
            // No segment need compaction
            if (recycleList.size() == 0) {
                _segPermits.set(0);
                return false;
            }
            
            // Sort recycleList in ascending order of load size
            Collections.sort(recycleList, _segmentLoadCmp);
            
            // Compact no more than 3 segments per compaction cycle.
            // The total of segment load factors need to be less than
            // 0.8 to allow 20% inaccuracy (for safety).
            double totalFactor = 0;
            for(int i = 0, len = Math.min(3, recycleList.size()); i < len; i++) {
                Segment seg = recycleList.get(i);
                if(totalFactor < 0.8) {
                    totalFactor += Math.max(0, seg.getLoadFactor());
                    if(totalFactor < 0.8) {
                        _segSourceList.add(seg);
                    }
                } else {
                    break;
                }
            }
            
            // Delay compaction if only one segment is eligible for compaction but it is not VERY fragmented.
            if (_segSourceList.size() == 1 && _segSourceList.get(0).getLoadFactor() > (_compactLoadFactor/2)) return false;
            
            try {
                for(Segment seg : _segSourceList) {
                    _log.info("Segment " + seg.getSegmentId() + " load factor=" + ((long)(seg.getLoadFactor() * 10000) / 10000.0));
                }
            } catch(ConcurrentModificationException e) {
                _segPermits.set(0);
                _segSourceList.clear();
                return false;
            }
            
            _segPermits.set(Math.max(_segSourceList.size() - 1, 0));
            _log.info("inspect done");
            return true;
        }
        
        /*
         * No synchronization on segManager is required after locating segments eligible for compaction.
         */
    }
    
    /**
     * Compacts a number of source fragmented Segments by moving data into a new target Segment.
     * 
     * @return <code>true</code> if this operation finished successfully. Otherwise, <code>false</code>.
     * 
     * @throws IOException if this operation can not be finished properly.
     */
    private boolean compact() throws IOException {
        try {
            _segTarget = _dataArray.getSegmentManager().nextSegment();
            for(Segment seg : _segSourceList) {
                try {
                    if(compact(seg, _segTarget)) {
                        _compactedQueue.add(seg);
                    }
                } catch(Exception e) {
                    _ignoredSegs.add(seg);
                    _log.error("failed to compact Segment " + seg.getSegmentId(), e);
                }
            }
            
            _targetQueue.add(_segTarget);
            _log.info("bytes transferred to   " + _segTarget.getSegmentId() + ": " + (_segTarget.getAppendPosition() - Segment.dataStartPosition));
        } catch(ConcurrentModificationException e1) {
            _segSourceList.clear();
            return false;
        } catch(Exception e2) {
            _log.warn(e2.getMessage(), e2);
            return false;
        }
        
        _log.info("compact done");
        return true;
    }
    
    /**
     * Compacts data from the specified source Segment into the specified target Segment.
     * 
     * @param segment   - the source Segment, from which data is read.
     * @param segTarget - the target Segment, to which data is written.
     * @return <code>true</code> if the source Segment is compacted successfully.
     *         Otherwise, <code>false</code>.
     * @throws IOException if this operation can not be finished properly.
     */
    private boolean compact(Segment segment, Segment segTarget) throws IOException {
        Segment segSource = segment; 
        int segSourceId = segSource.getSegmentId();
        int segTargetId = segTarget.getSegmentId();
        
        Chronos c = new Chronos();
        if(!segment.canReadFromBuffer() && segment.getLoadFactor() > 0.1) {
            segSource = new BufferedSegment(segment, getByteBuffer((int)segment.getInitialSize()));
            _log.info("buffering time: " + c.tick() + " ms");
        }
        
        long sizeLimit = segTarget.getInitialSize();
        long bytesTransferred = 0;
        boolean succ = true;
        
        try {
            AddressFormat addrFormat = _dataArray._addressFormat;
            
            for(int index = 0, cnt = _dataArray.length(); index < cnt; index++) {
                long oldAddress = _dataArray.getAddress(index);
                int oldSegPos = addrFormat.getOffset(oldAddress);
                int oldSegInd = addrFormat.getSegment(oldAddress);
                int length = addrFormat.getDataSize(oldAddress);
                
                if (oldSegInd == segSourceId && oldSegPos >= Segment.dataStartPosition) {
                    if(length == 0) length = segSource.readInt(oldSegPos);
                    int byteCnt = 4 + length;
                    long newSegPos = segTarget.getAppendPosition();
                    long newAddress = addrFormat.composeAddress((int)newSegPos, segTargetId, length);
                    
                    if(segTarget.getAppendPosition() + byteCnt >= sizeLimit) {
                        succ = false;
                        break;
                    }
                    
                    // Transfer bytes from source to target
                    segSource.transferTo(oldSegPos, byteCnt, segTarget);
                    bytesTransferred += byteCnt;
                    
                    _updateManager.addUpdate(index, byteCnt, newAddress, oldAddress, segTarget);
                }
            }
            
            // Push whatever left into update queue
            _updateManager.endUpdate(segTarget);
            _log.info("bytes transferred from " + segSource.getSegmentId() + ": " + bytesTransferred + " time: " + c.tick() + " ms");
            
            segTarget.force();
            return succ;
        } finally {
            if(segSource.getClass() == BufferedSegment.class) {
                segSource.close(false);
                segSource = null;
            }
        }
    }
    
    /**
     * Compacting Segments.
     */
    @Override
    public void run() {
        while(_enabled) {
            if(_newCycle.compareAndSet(true, false)) {
                // One and only one compactor is at work.
                _lock.lock();
                
                try {
                    reset();
                    _state = State.INIT;
                    _log.info("cycle init");
                    
                    // Inspect the array
                    if(!inspect()) continue;
                    
                    // Compact the array
                    if(!compact()) continue;
                } catch(Exception e) {
                    _log.error("failed to compact: " + e.getMessage(), e);
                } finally {
                    reset();
                    _state = State.DONE;
                    _log.info("cycle done");
                    _lock.unlock();
                }
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    _log.warn(e.getMessage());
                }
            }
        }
    }
    
    /**
     * Note that this method is called only by SimpleDataArray.
     */
    final void start() {
        _enabled = true;
        _ignoredSegs.clear();
        _executor = Executors.newSingleThreadExecutor(new CompactorThreadFactory());
        _executor.execute(this);
    }
    
    /**
     * Note that this method is called only by SimpleDataArray.
     */
    final void shutdown() {
        _enabled = false;
        _ignoredSegs.clear();
        
        if(_executor != null && !_executor.isShutdown()) {
            try {
                _executor.awaitTermination(_shutdownTimeout, TimeUnit.MILLISECONDS);
                _log.info("compactor shutdown");
            } catch (InterruptedException e) {
                _log.warn("compactor shutdown interrupted");
            }
            
            try {
                _state = State.DONE;
                _executor.shutdown();
            } catch (Exception e) {
                _log.warn("compactor shutdown forced");
            } finally {
                _executor = null;
            }
        }
    }
    
    /**
     * Note that this method is called only by SimpleDataArray.
     */
    final boolean isStarted() {
        return _state != State.DONE;
    }
    
    /**
     * Note that this method is called only by SimpleDataArray.
     */
    final void clear() {
        reset();
        
        _targetQueue.clear();
        _compactedQueue.clear();
        while(!_updateManager.isServiceQueueEmpty()) {
            CompactionUpdateBatch batch = _updateManager.pollBatch();
            _updateManager.recycleBatch(batch);
        }
    }
    
    /**
     * Reset internal data structures before the next compaction cycle.
     */
    private final void reset() {
        _segTarget = null;
        _segPermits.set(0);
        _segSourceList.clear();
        _updateManager.clear();
    }
    
    /**
     * Retrieves, but does not remove, the target Segment.
     */
    protected Segment peekTargetSegment() {
        return _targetQueue.peek();
    }

    /**
     * Retrieves and removes the target Segment.
     */
    protected Segment pollTargetSegment() {
        return _targetQueue.poll();
    }
    
    /**
     * Retrieves and removes the next {@link CompactionUpdateBatch}.
     */
    protected CompactionUpdateBatch pollCompactionBatch() {
        return _updateManager.pollBatch();
    }
    
    /**
     * Recycles a {@link CompactionUpdateBatch} after it is being consumed.
     * 
     * @param batch - the consumed CompactionUpdateBatch.
     * @return <code>true</code> if recycling is successful.
     */
    protected boolean recycleCompactionBatch(CompactionUpdateBatch batch) {
        return _updateManager.recycleBatch(batch);
    }
    
    /**
     * Gets the buffer for speeding up compaction.
     * 
     * @param bufferLength - the length of buffer.
     */
    protected ByteBuffer getByteBuffer(int bufferLength) {
        if(_buffer == null) {
            _buffer = ByteBuffer.wrap(new byte[bufferLength]);
            _log.info("ByteBuffer allocated for buffering");
        }
        
        return _buffer;
    }
    
    /**
     * Gets the queue of compacted Segments.
     */
    final ConcurrentLinkedQueue<Segment> getCompactedQueue() {
        return _compactedQueue;
    }
    
    /**
     * Checks whether there is still a permit for instantiating a new target Segment. 
     */
    final boolean getAndDecrementSegmentPermit() {
        return _segPermits.getAndDecrement() > 0;
    }
    
    /**
     * Gets the target Segment to which data is transferred.
     */
    final Segment getTargetSegment() {
        return _segTarget;
    }
    
    /**
     * Starts a new compaction cycle.
     */
    final void startsCycle() {
        _newCycle.set(true);
    }
    
    /**
     * The internal State of the Segment compactor.
     */
    static enum State {
        /**
         * The compaction cycle is initiated.
         */
        INIT,
        /**
         * The compaction cycle is finished.
         */
        DONE;
    }
    
    /**
     * CompactionUpdate specifies how data is moved during compaction.
     */
    static class CompactionUpdate {
        int _index;
        int _dataSize;
        long _dataAddr;
        long _origAddr;
        
        /**
         * Creates a new instance of CompactionUpdate.
         * 
         * @param index    - the array index where data is to be moved from.
         * @param dataSize - the size of data to be moved.
         * @param dataAddr - the address of the target Segment where data is moved to.
         * @param origAddr - the address of the source Segment where data is to be moved from.
         */
        CompactionUpdate(int index, int dataSize, long dataAddr, long origAddr) {
            this._index = index;
            this._dataSize = dataSize;
            this._dataAddr = dataAddr;
            this._origAddr = origAddr;
        }
        
        /**
         * Gets the string representation of this CompactionUpdate.
         */
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            
            buf.append(getClass().getSimpleName());
            buf.append("{index=");
            buf.append(_index);
            buf.append(",  dataSize=");
            buf.append(_dataSize);
            buf.append(",  dataAddr=");
            buf.append(_dataAddr);
            buf.append(",  origAddr=");
            buf.append(_origAddr);
            buf.append("}");
            
            return buf.toString();
        }
    }
    
    /**
     * CompactionUpdateBatch defines a CompactionUpdate container based on {@link ByteBuffer}
     * to minimize the impact of the Object overhead. It is a tradeoff of efficiency for GC.
     */
    static class CompactionUpdateBatch {
        static int _counter = 0;
        final int _batchId;
        final int _capacity;
        final int _unitSize = 24;
        final ByteBuffer _buffer;
        
        Segment _segTarget = null;
        int _dataSizeTotal = 0;
        int _serviceId = 0;
        long _lwMark = 0;
        
        /**
         * Creates a new instance of CompactionUpdateBatch.
         * @param capacity - the capacity of the batch.
         */
        CompactionUpdateBatch(int capacity) {
            this._capacity = capacity;
            this._batchId = _counter++;
            this._buffer = ByteBuffer.allocate(_capacity * _unitSize);
            _log.info("CompactionUpdateBatch " + _batchId);
        }
        
        /**
         * Clears the batch for reuse.
         */
        public void clear() {
            _buffer.clear();
            _segTarget = null;
            _dataSizeTotal = 0;
            _serviceId = 0;
            _lwMark = 0;
        }
        
        /**
         * Gets the batch capacity in the unit of {@link CompactionUpdate}.
         */
        public int getCapacity() {
            return _capacity;
        }
        
        /**
         * Gets the batch capacity in the unit of <code>byte</code>.
         */
        public int getByteCapacity() {
            return _buffer.capacity();
        }
        
        /**
         * Gets the internal {@link ByteBuffer}.
         */
        public ByteBuffer getInternalBuffer() {
            return _buffer;
        }
        
        /**
         * Gets the current size (i.e. the number of {@link CompactionUpdate}s in the batch)
         * @return
         */
        public int size() {
            return _buffer.position()/_unitSize;
        }
        
        /**
         * Checks if the batch is empty.
         */
        public boolean isEmpty() {
            return _buffer.position() == 0;
        }
        
        /**
         * Gets the batch Id.
         */
        public int getBatchId() {
            return _batchId;
        }
        
        /**
         * Gets the service Id of the batch.
         * <p>
         * A batch can be reused for compaction services many times.
         * </p>
         */
        public int getServiceId() {
            return _serviceId;
        }
        
        /**
         * Gets the descriptive Id of the batch for the purpose of debugging and logging. 
         * The descriptive Id is in the form below:
         * <pre>
         *  &lt;TargetSegementId&gt;[&lt;BatchServiceId&gt;]
         * </pre>
         */
        public String getDescriptiveId() {
            return ((_segTarget == null) ? "?[" : (_segTarget.getSegmentId() + "[")) + _serviceId + "]";
        }
        
        /**
         * Gets the low water mark of this batch.
         */
        public long getLWMark() {
            return _lwMark;
        }
        
        /**
         * Gets the target Segment to which the updates inside this batch are applied.
         */
        public Segment getTargetSegment() {
            return _segTarget;
        }
        
        /**
         * Add a new {@link CompactionUpdate}.
         * 
         * @param index    - the array index where data is to be moved from.
         * @param dataSize - the size of data to be moved.
         * @param dataAddr - the address of the target Segment where data is moved to.
         * @param origAddr - the address of the source Segment where data is to be moved from.
         */
        public void add(int index, int dataSize, long dataAddr, long origAddr) {
            _buffer.putInt(index);
            _buffer.putInt(dataSize);
            _buffer.putLong(dataAddr);
            _buffer.putLong(origAddr);
            _dataSizeTotal += dataSize;
        }
        
        /**
         * Gets the {@link CompactionUpdate} at the specified index into the batch.
         * 
         * @param i - the index into the batch.
         */
        public CompactionUpdate get(int i) {
            return new CompactionUpdate(getUpdateIndex(i),
                                        getUpdateDataSize(i),
                                        getUpdateDataAddr(i),
                                        getOriginDataAddr(i));
        }
        
        /**
         * Gets the array index of the {@link CompactionUpdate} at the specified index into the batch.
         * 
         * @param i - the index into the batch.
         */
        public int getUpdateIndex(int i) {
            return _buffer.getInt(i * _unitSize);
        }
        
        /**
         * Gets the data size of the {@link CompactionUpdate} at the specified index into the batch.
         * 
         * @param i - the index into the batch.
         */
        public int getUpdateDataSize(int i) {
            return _buffer.getInt((i * _unitSize) + 4);
        }
        
        /**
         * Gets the update address of the {@link CompactionUpdate} at the specified index into the batch.
         * 
         * @param i - the index into the batch.
         */
        public long getUpdateDataAddr(int i) {
            return _buffer.getLong((i * _unitSize) + 8);
        }
        
        /**
         * Gets the original address of the {@link CompactionUpdate} at the specified index into the batch.
         * 
         * @param i - the index into the batch.
         */
        public long getOriginDataAddr(int i) {
            return _buffer.getLong((i * _unitSize) + 16);
        }
        
        /**
         * Gets the total size of transferred data in the unit of <code>byte</code> known to this batch.
         */
        public int getDataSizeTotal() {
            return _dataSizeTotal;
        }
        
        /**
         * Sets the low water mark of this batch when it is filled up.
         */
        void setLWMark(long waterMark) {
            _lwMark = waterMark;
        }
        
        /**
         * Sets the target Segment of this batch.
         */
        void setTargetSegment(Segment seg) {
            _segTarget = seg;
        }
        
        /**
         * Sets the service Id of this batch.
         */
        void setServiceId(int serviceId) {
            _serviceId = serviceId;
        }
    }
    
    /**
     * CompactionUpdateManager manages the communication of compaction updates
     * between the {@link SimpleDataArray} and the associated Segment compactor.
     */
    static class CompactionUpdateManager {
        /**
         * The size of {@link CompactionUpdateBatch}.
         */
        private final int _batchSize;
        
        /**
         * The service queue of {@link CompactionUpdateBatch} to be consumed.
         */
        private final ConcurrentLinkedQueue<CompactionUpdateBatch> _serviceBatchQueue;
        
        /**
         * The recycle queue of {@link CompactionUpdateBatch} to be reused.
         */
        private final ConcurrentLinkedQueue<CompactionUpdateBatch> _recycleBatchQueue;
        
        /**
         * The data array to be compacted.
         */
        private final SimpleDataArray _dataArray;
        
        /**
         * The counter of batch service Id.
         */
        private int _batchServiceIdCounter = 0;
        
        /**
         * The current batch to which a {@link CompactionUpdate} is added.
         */
        private CompactionUpdateBatch _batch;
        
        /**
         * Creates a new instance of CompactionUpdateManager.
         * 
         * @param dataArray - the data array to be compacted.
         * @param batchSize - the size of {@link CompactionUpdateBatch}.
         */
        public CompactionUpdateManager(SimpleDataArray dataArray, int batchSize) {
            _dataArray = dataArray;
            _batchSize = batchSize;
            _serviceBatchQueue = new ConcurrentLinkedQueue<CompactionUpdateBatch>();
            _recycleBatchQueue = new ConcurrentLinkedQueue<CompactionUpdateBatch>();
            nextBatch();
        }
        
        /**
         * Gets the next batch to used for compaction.
         */
        private void nextBatch() {
            _batch = _recycleBatchQueue.poll();
            if(_batch == null) {
                _batch = new CompactionUpdateBatch(_batchSize);
            }
            
            _batch.clear();
            _batch.setServiceId(_batchServiceIdCounter++);
        }
        
        /**
         * Checks if the service queue is empty.
         */
        public boolean isServiceQueueEmpty() {
            return _serviceBatchQueue.isEmpty();
        }
        
        /**
         * Checks if the recycle queue is empty.
         */
        public boolean isRecycleQueueEmpty() {
            return _recycleBatchQueue.isEmpty();
        }
        
        /**
         * Retrieves and removes a {@link CompactionUpdateBatch} from the service queue.
         */
        public CompactionUpdateBatch pollBatch() {
            return _serviceBatchQueue.poll();
        }
        
        /**
         * Clears and adds a {@link CompactionUpdateBatch} to the recycle queue.
         * 
         * @param batch - the batch to be recycled.
         * @return <code>true</code> if the batch is recycled.
         */
        public boolean recycleBatch(CompactionUpdateBatch batch) {
            batch.clear();
            return _recycleBatchQueue.add(batch);
        }
        
        /**
         * Adds a new {@link CompactionUpdate}.
         * 
         * @param index     - the array index where data is to be moved from.
         * @param dataSize  - the size of data to be moved.
         * @param dataAddr  - the address of the target Segment where data is moved to.
         * @param origAddr  - the address of the source Segment where data is to be moved from.
         * @param segTarget - the target Segment to which data is transferred.
         * @throws IOException if the compaction update can not be added.
         */
        public void addUpdate(int index, int dataSize, long dataAddr, long origAddr, Segment segTarget) throws IOException {
            try {
                _batch.add(index, dataSize, dataAddr, origAddr);
            } catch(BufferOverflowException e) {
                segTarget.force();
                _batch.setTargetSegment(segTarget);
                _batch.setLWMark(_dataArray.getLWMark());
                _log.info("compaction batch " + _batch.getDescriptiveId() + " hwMark=" + _batch.getLWMark());
                
                _serviceBatchQueue.add(_batch);
                nextBatch();
                
                // Add compaction update to new batch
                _batch.add(index, dataSize, dataAddr, origAddr);
            }
        }
        
        /**
         * Flushes compaction updates to the target Segment.
         * 
         * @param segTarget - the target Segment to which data is transferred.
         * @throws IOException if this operation failed for any reasons.
         */
        public void endUpdate(Segment segTarget) throws IOException {
            segTarget.force();
            _batch.setTargetSegment(segTarget);
            _batch.setLWMark(_dataArray.getLWMark());
            _log.info("compaction batch " + _batch.getDescriptiveId() + " hwMark=" + _batch.getLWMark());
            
            _serviceBatchQueue.add(_batch);
            _batchServiceIdCounter = 0;
            nextBatch();
        }
        
        /**
         * Clears the internal state of this CompactionUpdateManager.
         */
        public void clear() {
            _batchServiceIdCounter = 0;
            _batch.clear();
            _batch.setServiceId(_batchServiceIdCounter++);
        }
    }
    
    /**
     * BufferedSegment loads a Segment into a {@link ByteBuffer} to fast access to segment data.
     */
    static class BufferedSegment extends MemorySegment {
        private ByteBuffer _byteBuffer = null;
        
        /**
         * Creates a new instance of BufferedSegment.
         * 
         * @param segment - the Segment to be wrapped.
         * @param buffer  - the byte buffer
         * @throws IOException if the instance cannot be created. 
         */
        public BufferedSegment(Segment segment, ByteBuffer buffer) throws IOException {
            super(segment.getSegmentId(), segment.getSegmentFile(), segment.getInitialSizeMB(), segment.getMode());
            this._byteBuffer = buffer;
            this.init();
        }
        
        @Override
        protected void init() throws IOException {
            if(_byteBuffer == null) return;
            super.init();
        }
        
        @Override
        protected ByteBuffer initByteBuffer() {
            _byteBuffer.clear();
            return _byteBuffer;
        }
    }
    
    /**
     * CompactorThreadFactory produces daemon threads for running compaction.
     */
    static class CompactorThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    }
}
