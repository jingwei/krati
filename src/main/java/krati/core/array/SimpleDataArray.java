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
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import krati.core.StoreParams;
import org.apache.log4j.Logger;

import krati.Mode;
import krati.Persistable;
import krati.array.Array;
import krati.array.DataArray;
import krati.array.LongArray;
import krati.core.array.SimpleDataArrayCompactor.CompactionUpdateBatch;
import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryPersistAdapter;
import krati.core.array.entry.EntryValue;
import krati.core.segment.AddressFormat;
import krati.core.segment.Segment;
import krati.core.segment.SegmentException;
import krati.core.segment.SegmentManager;
import krati.core.segment.SegmentOverflowException;
import krati.io.Closeable;

/**
 * SimpleDataArray: Simple Persistent DataArray.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SimpleDataArray for a given home directory.
 *    2. There is one and only one thread calling setData and transferTo methods at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 * 
 * <p>
 * 05/09, 2011 - added support for Closeable
 * 05/22, 2011 - fixed method close()
 * 05/23, 2011 - sync compaction batches in method close()
 * 05/26, 2011 - added methods for partially reading data bytes
 * 06/22, 2011 - catch SegmentException in method set() for safety
 */
public class SimpleDataArray implements DataArray, Persistable, Closeable {
    private final static Logger _log = Logger.getLogger(SimpleDataArray.class);
    
    protected final AddressArray _addressArray;
    protected final AddressFormat _addressFormat;
    protected final SegmentManager _segmentManager;
    protected final SimpleDataArrayCompactor _compactor;
    protected final double _segmentCompactFactor;
    
    /**
     * Current working segment to append data to.
     */
    private volatile Segment _segment;
    
    /**
     * The mode can only be <code>Mode.INIT</code>, <code>Mode.OPEN</code> and <code>Mode.CLOSED</code>.
     */
    private volatile Mode _mode = Mode.INIT;
    
    /**
     * Append position triggering segment meta data update
     */
    private volatile long _metaUpdatePosition = Segment.dataStartPosition;
    
    /**
     * Constructs a DataArray with Segment Compact Factor default to 0.5. 
     * 
     * @param addressArray           the array of addresses (i.e. pointers to Segment).
     * @param segmentManager         the segment manager for loading, creating, freeing, maintaining segments.
     */
    public SimpleDataArray(AddressArray addressArray, SegmentManager segmentManager) {
        this(addressArray, segmentManager, StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT);
    }
    
    /**
     * Constructs a DataArray.
     * 
     * @param addressArray           the array of addresses (i.e. pointers to Segment).
     * @param segmentManager         the segment manager for loading, creating, freeing, maintaining segments.
     * @param segmentCompactFactor   the load factor below which a segment is eligible for compaction. The recommended value is 0.5.
     */
    public SimpleDataArray(AddressArray addressArray,
                           SegmentManager segmentManager,
                           double segmentCompactFactor) {
        this._addressArray = addressArray;
        this._segmentManager = segmentManager;
        this._segmentCompactFactor = segmentCompactFactor;
        this._addressFormat = new AddressFormat();
        
        // Add segment persist listener
        addressArray.setPersistListener(new SegmentPersistListener());
        
        // Start segment data compactor
        _compactor = new SimpleDataArrayCompactor(this, getSegmentCompactFactor());
        _compactor.start();
        
        this.init();
        this._mode = Mode.OPEN;
        _log.info("mode=" + _mode);
    }
    
    private void consumeCompaction(CompactionUpdateBatch updateBatch) throws Exception {
        int ignoreCount = 0;
        int updateCount = updateBatch.size();
        int totalIgnoreBytes = 0;
        int totalUpdateBytes = updateBatch.getDataSizeTotal();
        
        Segment segTarget = updateBatch.getTargetSegment();
        
        for(int i = 0; i < updateCount; i++) {
            int index = updateBatch.getUpdateIndex(i);
            long origAddr = updateBatch.getOriginDataAddr(i);
            long currAddr = getAddress(index);
            
            if(currAddr == 0 ||      /* data at the given index is deleted by writer */ 
               currAddr != origAddr  /* data at the given index is updated by writer */) {
                /*
                 * The address generated by the compactor is obsolete.
                 */
                int updateBytes = updateBatch.getUpdateDataSize(i);
                totalIgnoreBytes += updateBytes;
                ignoreCount++;
            } else {
                /*
                 * The address generated by the compactor has not been touched by the writer.
                 * Update the address array directly.
                 */ 
                setCompactionAddress(index, updateBatch.getUpdateDataAddr(i), updateBatch.getLWMark());
            }
        }
        
        int consumeCount = updateCount - ignoreCount;
        int totalConsumeBytes = totalUpdateBytes - totalIgnoreBytes;
        
        _log.info("consumed compaction batch " + updateBatch.getDescriptiveId() +
                  " updates " + consumeCount + "/" + updateCount +
                  " bytes " + totalConsumeBytes + "/" + totalUpdateBytes);
        
        // Update segment load size
        segTarget.decrLoadSize(totalIgnoreBytes);
        _log.info("Segment " + segTarget.getSegmentId() + " catchup " + segTarget.getStatus());
    }
    
    protected boolean consumeCompactionBatch() {
        /*
         * Consume one compaction update batch generated by the compactor.
         */
        CompactionUpdateBatch updateBatch = _compactor.pollCompactionBatch();
        if(updateBatch != null) {
            try {
                consumeCompaction(updateBatch);
            } catch (Exception e) {
                _log.error("failed to consume compaction batch " + updateBatch.getDescriptiveId(), e);
            } finally {
                _compactor.recycleCompactionBatch(updateBatch);
            }
            return true;
        }
        
        return false;
    }
    
    protected void consumeCompactionBatches() {
        while(true) {
            CompactionUpdateBatch updateBatch = _compactor.pollCompactionBatch();
            if(updateBatch == null) break;
            
            try {
                consumeCompaction(updateBatch);
            } catch (Exception e) {
                _log.error("failed to consume compaction batch " + updateBatch.getDescriptiveId(), e);
            } finally {
                _compactor.recycleCompactionBatch(updateBatch);
            }
        }
    }
    
    protected void syncCompactor() {
        ConcurrentLinkedQueue<Segment> queue = _compactor.getCompactedQueue();
        while(!queue.isEmpty()) {
            Segment seg = queue.remove();
            try {
                consumeCompactionBatches();
                _segmentManager.freeSegment(seg);
            } catch(IOException e) {
                _log.error("failed to free Segment " + seg.getSegmentId() + ": " + seg.getStatus(), e);
            }
        }
        
        consumeCompactionBatches();
    }
    
    protected void init() {
        try {
            _metaUpdatePosition = Segment.dataStartPosition;
            _segment = _segmentManager.nextSegment();
            _compactor.startsCycle();
            
            _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
        } catch(IOException ioe) {
            _log.error(ioe.getMessage(), ioe);
            throw new SegmentException("Instantiation failed due to " + ioe.getMessage());
        }
    }
    
    public final AddressFormat getAddressFormat() {
        return _addressFormat;
    }
    
    public final long getAddress(int index) {
        return _addressArray.get(index);
    }
    
    protected void setAddress(int index, long value, long scn) throws Exception {
        _addressArray.set(index, value, scn);
    }
    
    protected void setCompactionAddress(int index, long value, long scn) throws Exception {
        _addressArray.setCompactionAddress(index, value, scn);
    }
    
    protected LongArray getAddressArray() {
        return _addressArray;
    }
    
    protected double getSegmentCompactFactor() {
        return _segmentCompactFactor;
    }
    
    protected SegmentManager getSegmentManager() {
        return _segmentManager;
    }
    
    protected Segment getCurrentSegment() {
        return _segment;
    }
    
    protected void decrOriginalSegmentLoad(int index) {
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            int length = _addressFormat.getDataSize(address);
            
            if (segPos >= Segment.dataStartPosition) {
                // get data segment
                Segment seg = _segmentManager.getSegment(segInd);
                
                // read data length
                if(seg != null) seg.decrLoadSize(4 + ((length == 0) ? seg.readInt(segPos) : length));
            }
        }
        catch(IOException e1) {}
        catch(IndexOutOfBoundsException e2) {}
    }
    
    private final void doThrottling(int lastWriteSize) {
        Segment writerSegment = _segment;
        if(writerSegment == null) {
            return;
        }
        
        Segment compactorTarget = _compactor.getTargetSegment();
        if(compactorTarget == null || compactorTarget == writerSegment) {
            return;
        }
        
        /*
         * Slow down the writer so that the compactor has a chance to catch up.
         */
        int writerLoadSize = writerSegment.getLoadSize();
        int targetLoadSize = compactorTarget.getLoadSize();
        if (targetLoadSize < writerLoadSize) {
            final long totalWait = 1; // milliseconds
            final long startTime = System.currentTimeMillis();
            targetLoadSize += (targetLoadSize == 0 ?
                                 (lastWriteSize * 2) :
                                 (int)((double)writerLoadSize / targetLoadSize * lastWriteSize));
            
            while(compactorTarget.getLoadSize() < targetLoadSize) {
                // Sleep 0.2 milliseconds only if no compaction batch was consumed
                if(!consumeCompactionBatch()) {
                    try {
                        Thread.sleep(0 /* milliseconds */, 200000 /* nanoseconds */);
                    } catch(Exception e) {}
                }
                
                long elapsedTime = System.currentTimeMillis() - startTime; 
                if (elapsedTime >= totalWait) {
                    _log.info("throttle " + elapsedTime + " ms");
                    return;
                }
            }
        }
    }
    
    private final void rangeCheck(int index) {
        if(!_addressArray.hasIndex(index)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
    }
    
    /**
     * @return <code>true</code> if this array has data at the given index. Otherwise, <code>false</code>.  
     * @throws ArrayIndexOutOfBoundsException if the index is out of range.
     */
    @Override
    public boolean hasData(int index) {
        rangeCheck(index);
        
        long address = getAddress(index);
        int segPos = _addressFormat.getOffset(address);
        int segInd = _addressFormat.getSegment(address);
        
        // no data found
        if(segPos < Segment.dataStartPosition) return false;
        
        // get data segment
        Segment seg = _segmentManager.getSegment(segInd);
        if(seg == null) return false;
        
        return true;
    }
    
    /**
     * @return the length of data at the given index.
     * If the given index is out of the array index range, <code>-1<code> is returned.
     */
    @Override
    public int getLength(int index) {
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            if(seg == null) return -1;
            
            // read data length
            int size = _addressFormat.getDataSize(address);
            return (size == 0) ? seg.readInt(segPos) : size;
        } catch(Exception e) {
            _log.warn(e.getMessage());
            return -1;
        }
    }
    
    /**
     * Gets data at a given index.
     * 
     * @param index  the array index
     * @return the data at a given index.
     * @throws ArrayIndexOutOfBoundsException if the index is out of range.
     */
    @Override
    public byte[] get(int index) {
        rangeCheck(index);
        
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return null;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            if(seg == null) return null;
            
            // read data length
            int size = _addressFormat.getDataSize(address);
            int len = (size == 0) ? seg.readInt(segPos) : size;
            
            // read data into byte array
            byte[] data = new byte[len];
            if (len > 0) {
                seg.read(segPos + 4, data);
            }
            
            return data;
        } catch(Exception e) {
            _log.warn(e.getMessage());
            return null;
        }
    }
    
    /**
     * Gets data at a given index.
     * 
     * @param index  the array index
     * @param data   the byte array to fill in
     * @return the length of data at the given index.
     * @throws ArrayIndexOutOfBoundsException if the index is out of range
     * or if the byte array does not have enough space to hold the read data.
     */
    @Override
    public int get(int index, byte[] data) {
        return get(index, data, 0);
    }
    
    /**
     * Gets data at a given index.
     * 
     * @param index  the array index
     * @param data   the byte array to fill in
     * @param offset the offset of the byte array where data is filled in 
     * @return the length of data at the given index.
     * @throws ArrayIndexOutOfBoundsException if the index is out of range
     * or if the byte array does not have enough space to hold the read data.
     */
    @Override
    public int get(int index, byte[] data, int offset) {
        rangeCheck(index);
        
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            if(seg == null) return -1;
            
            // read data length
            int size = _addressFormat.getDataSize(address);
            int len = (size == 0) ? seg.readInt(segPos) : size;
            
            // read data into byte array
            if (len > 0) {
                seg.read(segPos + 4, data, offset, len);
            }
            
            return len;
        } catch(Exception e) {
            _log.warn(e.getMessage());
            return -1;
        }
    }
    
    /**
     * Reads data bytes at an index into a byte array.
     * 
     * This method does a full read of data bytes only if the destination byte
     * array has enough capacity to store all the bytes from the specified index.
     * Otherwise, a partial read is done to fill in the destination byte array.
     *   
     * @param index  the array index
     * @param dst    the byte array to fill in
     * @return the total number of bytes read if data is available at the given index.
     *         Otherwise, <code>-1</code>.
     */
    public int read(int index, byte[] dst) {
        rangeCheck(index);
        
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            if(seg == null) return -1;
            
            // read data length
            int size = _addressFormat.getDataSize(address);
            int len = (size == 0) ? seg.readInt(segPos) : size;
            
            // read data into byte array
            if (len > 0) {
                len = Math.min(len, dst.length);
                seg.read(segPos + 4, dst, 0, len);
            }
            
            return len;
        } catch(Exception e) {
            _log.warn(e.getMessage());
            return -1;
        }
    }
    
    /**
     * Reads data bytes from an offset of data at an index to fill in a byte array.
     *   
     * @param index  the array index
     * @param offset the offset of data bytes
     * @param dst    the byte array to fill in
     * @return the total number of bytes read if data is available at the given index.
     *         Otherwise, <code>-1</code>.
     */
    public int read(int index, int offset, byte[] dst) {
        rangeCheck(index);
        
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            if(seg == null) return -1;
            
            // read data length
            int size = _addressFormat.getDataSize(address);
            int len = (size == 0) ? seg.readInt(segPos) : size;
            
            // read data into byte array
            if (len > 0) {
                if (len > offset) {
                    len = Math.min(len - offset, dst.length);
                    seg.read(segPos + 4 + offset, dst, 0, len);
                } else {
                    return -1;
                }
            }
            
            return len;
        } catch(Exception e) {
            _log.warn(e.getMessage());
            return -1;
        }
    }
    
    /**
     * Transfers data at a given index to a writable channel.
     * 
     * @param index  the array index
     * @return the amount of bytes transferred (the length of data at the given index).
     * @throws ArrayIndexOutOfBoundsException if the index is out of range.
     */
    @Override
    public int transferTo(int index, WritableByteChannel channel) {
        rangeCheck(index);
        
        try {
            long address = getAddress(index);
            int segPos = _addressFormat.getOffset(address);
            int segInd = _addressFormat.getSegment(address);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            if(seg == null) return -1;
            
            // read data length
            int size = _addressFormat.getDataSize(address);
            int len = (size == 0) ? seg.readInt(segPos) : size;
            
            // transfer data to a writable channel
            if (len > 0) {
                seg.transferTo(segPos + 4, len, channel);
            }
            
            return len;
        } catch(Exception e) {
            return -1;
        }
    }
    
    /**
     * Sets data at a given index.
     * 
     * @param index  the array index
     * @param data   the data (byte array).
     *               If <code>null</code>, the data at the given index will be removed.
     * @param scn    the global scn indicating the sequence of this change
     * @throws ArrayIndexOutOfBoundsException if the index is out of range.
     */
    @Override
    public void set(int index, byte[] data, long scn) throws Exception {
        if(data == null) {
            set(index, data, 0, 0, scn);
        } else {
            set(index, data, 0, data.length, scn);
        }
    }
    
    /**
     * Sets data at a given index.
     * 
     * @param index  the array index
     * @param data   the data (byte array)
     *               If <code>null</code>, the data at the given index will be removed.
     * @param offset the offset of byte array where data is read
     * @param length the length of data to read from the byte array
     * @param scn    the global scn indicating the sequence of this change
     * @throws ArrayIndexOutOfBoundsException if the index is out of range
     * or if the offset and length is not properly specified.
     */
    @Override
    public void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        rangeCheck(index);
        decrOriginalSegmentLoad(index);
        
        // no data
        if (data == null) {
            setAddress(index, 0, scn);
            return;
        }
        
        if (offset > data.length || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(data.length);
        }
        
        while(true) {
            // get append position
            long pos = _segment.getAppendPosition();
            
            try {
                // check append position is in range
                if ((pos >> _addressFormat.getSegmentShift()) > 0) {
                    throw new SegmentOverflowException(_segment);
                }
                
                // append actual size
                _segment.appendInt(length);
                
                // append actual data
                if (length > 0) {
                    _segment.append(data, offset, length);
                }
                
                // update addressArray 
                long address = _addressFormat.composeAddress((int)pos, _segment.getSegmentId(), length);
                setAddress(index, address, scn);
                
                // update segment meta on first write
                if (pos >= _metaUpdatePosition) {
                    _segmentManager.updateMeta();
                    _metaUpdatePosition = _segment.getInitialSize();
                }
                
                if(_compactor.isStarted()) {
                    consumeCompactionBatch();
                    
                    /*
                     * Throttle the writer to average-out write latency.
                     * Give the compactor a chance to catch up with the writer.
                     */
                    doThrottling(length + 4);
                }
                
                return;
            } catch(SegmentException se) {
                _log.info("Segment " + _segment.getSegmentId() + " filled: " + _segment.getStatus());
                
                Segment nextSegment = _compactor.peekTargetSegment();
                if(nextSegment != null) {
                    persist();
                    
                    // get the next segment available for appending
                    _segment = nextSegment;
                    _compactor.pollTargetSegment();
                    _metaUpdatePosition = _segment.getInitialSize();
                    
                    _log.info("nextSegment from compactor");
                    _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
                } else {
                    if(_compactor.isStarted()) {
                        if(_compactor.getAndDecrementSegmentPermit()) {
                            _log.info("nextSegment permit granted");
                            
                            persist();
                            
                            // get the next segment available for appending
                            _metaUpdatePosition = Segment.dataStartPosition;
                            _segment = _segmentManager.nextSegment();
                            
                            _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
                        } else {
                            _log.info("nextSegment permit refused");
                            
                            // wait until compactor is done
                            while(_compactor.isStarted()) {
                                consumeCompactionBatch();
                                
                                _log.info("wait for compactor");
                                Thread.sleep(10);
                            }
                            
                            persist();
                            
                            // get the next segment available for appending
                            _segment = _compactor.pollTargetSegment();
                            if(_segment == null) {
                                _segment = _segmentManager.nextSegment();
                                _metaUpdatePosition = Segment.dataStartPosition;
                            } else {
                                _metaUpdatePosition = _segment.getInitialSize();
                            }
                            
                            _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
                        }
                    } else {
                        _log.info("nextSegment");
                        
                        persist();
                        
                        // get the next segment available for appending
                        _metaUpdatePosition = Segment.dataStartPosition;
                        _segment = _segmentManager.nextSegment();
                        _compactor.startsCycle();
                        
                        _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
                    }
                }
            } catch(Exception e) {
                // restore append position 
                _segment.setAppendPosition(pos);
                _segment.force();
                
                throw e;
            }
        }
    }
    
    @Override
    public boolean hasIndex(int index) {
        return _addressArray.hasIndex(index);
    }
    
    @Override
    public int length() {
        return _addressArray.length();
    }
    
    @Override
    public long getLWMark() {
        return _addressArray.getLWMark();
    }
    
    @Override
    public long getHWMark() {
        return _addressArray.getHWMark();
    }
    
    @Override
    public synchronized void saveHWMark(long endOfPeriod) throws Exception {
        if(isOpen()) {
            _addressArray.saveHWMark(endOfPeriod);
        }
    }
    
    @Override
    public synchronized void sync() throws IOException {
        if(isOpen()) {
            syncCompactor();
            
            /* CALLS ORDERED: Need force _segment first and then persist
             * _addressArray. During recovery, the _addressArray can always
             * point to addresses which are valid though may not reflect the
             * most recent address update.
             */
            _segment.force();
            _addressArray.sync();
            _segmentManager.updateMeta();
        }
    }
    
    @Override
    public synchronized void persist() throws IOException {
        if(isOpen()) {
            syncCompactor();
            
            /* CALLS ORDERED: Need force _segment first and then persist
             * _addressArray. During recovery, the _addressArray can always
             * point to addresses which are valid though may not reflect the
             * most recent address update.
             */
            _segment.force();
            _addressArray.persist();
            _segmentManager.updateMeta();
        }
    }
    
    /**
     * Clears all data stored in this SimpleDataArray.
     * This method is not effective if this SimpleDataArray is not open. 
     */
    @Override
    public synchronized void clear() {
        if(isOpen()) {
            _compactor.clear();
            _addressArray.clear();
            _segmentManager.clear();
            this.init();
        }
    }
    
    /**
     * Close to quit from serving requests.
     * 
     * @throws IOException if the underlying service cannot be closed properly.
     */
    @Override
    public synchronized void close() throws IOException {
        if (_mode == Mode.CLOSED) {
            return;
        }
        
        _mode = Mode.CLOSED;
        
        try {
            // THE CALLS ORDERED. 
            _compactor.shutdown();   // shutdown compactor
            sync();                  // consume compaction batches are generated during shutdown
            _compactor.clear();      // cleanup compactor internal state
            _addressArray.close();   // close address array
            _segmentManager.close(); // close segment manager
        } catch(Exception e) {
            _log.error("Failed to close", e);
            throw (e instanceof IOException) ? (IOException)e : new IOException(e);
        } finally {
            _log.info("mode=" + _mode);
        }
    }
    
    /**
     * Open to start serving requests.
     *  
     * @throws IOException if the underlying service cannot be opened properly.
     */
    @Override
    public synchronized void open() throws IOException {
        if (_mode == Mode.OPEN) {
            return;
        }
        
        try {
            _addressArray.open();
            _segmentManager.open();
            _compactor.start();
            
            init();
            _mode = Mode.OPEN;
        } catch(Exception e) {
            _mode = Mode.CLOSED;
            _log.error("Failed to open", e);
            
            _compactor.shutdown();
            _compactor.clear();
            
            if (_addressArray.isOpen()) {
                _addressArray.close();
            }
            if (_segmentManager.isOpen()) {
                _segmentManager.close();
            }
            
            throw (e instanceof IOException) ? (IOException)e : new IOException(e);
        } finally {
            _log.info("mode=" + _mode);
        }
    }
    
    @Override
    public boolean isOpen() {
        return _mode == Mode.OPEN;
    }
    
    private class SegmentPersistListener extends EntryPersistAdapter {
        
        public void beforePersist(Entry<? extends EntryValue> e) throws IOException {
            if(_segment != null) {
                _segment.force();
            }
        }
        
        public void afterPersist(Entry<? extends EntryValue> e) throws IOException {
            if(_segmentManager != null) {
                _segmentManager.updateMeta();
            }
        }
    }

    @Override
    public final Array.Type getType() {
        return _addressArray.getType();
    }
}
