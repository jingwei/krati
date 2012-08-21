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

package krati.core.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import krati.Mode;
import krati.array.Array;
import krati.array.DynamicArray;
import krati.core.OperationAbortedException;
import krati.core.array.AddressArray;
import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryLongFactory;
import krati.core.array.entry.EntryPersistListener;
import krati.core.array.entry.EntryValueLong;

/**
 * IOTypeLongArray
 * 
 * @author jwu
 * 06/12, 2011
 * 
 * <p>
 * 06/24, 2011 - Fixed the water marks of underlying ArrayFile. <br/>
 */
public class IOTypeLongArray extends AbstractRecoverableArray<EntryValueLong> implements AddressArray, DynamicArray {
    private final static int _subArrayBits = DynamicConstants.SUB_ARRAY_BITS;
    private final static int _subArraySize = DynamicConstants.SUB_ARRAY_SIZE;
    private final static Logger _logger = Logger.getLogger(IOTypeLongArray.class);
    private final Array.Type _type;
    private float _expandRate = 0;
    
    /**
     * The mode can only be <code>Mode.INIT</code>, <code>Mode.OPEN</code> and <code>Mode.CLOSED</code>.
     */
    private volatile Mode _mode = Mode.INIT;
    
    public IOTypeLongArray(Array.Type type, int length, int batchSize, int numSyncBatches, File directory) throws Exception {
        super(length, 8 /* elementSize */, batchSize, numSyncBatches, directory, new EntryLongFactory());
        this._type = (type != null) ? type : Array.Type.DYNAMIC;
        this._mode = Mode.OPEN;
    }
    
    @Override
    protected Logger getLogger() {
        return _logger;
    }
    
    @Override
    protected void loadArrayFileData() {
        long maxScn = _arrayFile.getLwmScn();
        _entryManager.setWaterMarks(maxScn, maxScn);
    }
    
    /**
     * Sync-up the high water mark to a given value.
     * 
     * @param endOfPeriod
     */
    @Override
    public void saveHWMark(long endOfPeriod) {
        if(getHWMark() < endOfPeriod) {
            try {
                set(0, get(0), endOfPeriod);
            } catch(Exception e) {
                _logger.error("Failed to saveHWMark " + endOfPeriod, e);
            }
        } else if(0 < endOfPeriod && endOfPeriod < getLWMark()) {
            try {
                _entryManager.sync();
            } catch(Exception e) {
                _logger.error("Failed to saveHWMark" + endOfPeriod, e);
            }
            _entryManager.setWaterMarks(endOfPeriod, endOfPeriod);
        }
    }
    
    @Override
    public void clear() {
        // Clear the entry manager
        _entryManager.clear();
        
        // Clear the underlying array file
        try {
            _arrayFile.resetAll(0L, _entryManager.getHWMark());
        } catch(IOException e) {
            _logger.error(e.getMessage(), e);
        }
    }
    
    protected long getPosition(int index) {
        return ArrayFile.ARRAY_HEADER_LENGTH + ((long)index << 3);
    }
    
    @Override
    public long get(int index) {
        if(index < 0 || index >= _length) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        
        try {
            return _arrayFile.getBasicIO().readLong(getPosition(index));
        } catch (IOException e) {
            throw new OperationAbortedException("Read aborted at index " + index, e);
        }
    }
    
    @Override
    public void set(int index, long value, long scn) throws Exception {
        if(index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        
        if(_type == Array.Type.DYNAMIC) {
            expandCapacity(index);
        } else if(_type == Array.Type.STATIC && index >= _length) {
            throw new ArrayIndexOutOfBoundsException(index);    
        }
        
        _arrayFile.getBasicIO().writeLong(getPosition(index), value);
        _entryManager.addToPreFillEntryLong(index, value, scn);
    }
    
    @Override
    public void setCompactionAddress(int index, long address, long scn) throws Exception {
        if(index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        
        expandCapacity(index);
        _arrayFile.getBasicIO().writeLong(getPosition(index), address);
        _entryManager.addToPreFillEntryLongCompaction(index, address, scn);
    }
    
    @Override
    public long[] getInternalArray() {
        try {
            return _arrayFile.loadLongArray();
        } catch (IOException e) {
            throw new OperationAbortedException(e);
        }
    }
    
    @Override
    public EntryPersistListener getPersistListener() {
      return getEntryManager().getPersistListener();
    }
    
    @Override
    public void setPersistListener(EntryPersistListener persistListener) {
        getEntryManager().setPersistListener(persistListener);
    }

    @Override
    public float getExpandRate() {
        return _expandRate;
    }
    
    @Override
    public void setExpandRate(float rate) {
        if(rate < 0 || rate > 1) {
            throw new IllegalArgumentException("invalid value: " + rate);
        }
        this._expandRate = rate;
    }
    
    @Override
    public void expandCapacity(int index) throws Exception {
        if(index < _length) return;
        
        // No expansion on static array
        if(_type == Array.Type.STATIC) {
            throw new UnsupportedOperationException("Array is of type " + _type);
        }
        
        // Choose the larger capacity between linear growth and exponential growth
        long capacity = ((index >> _subArrayBits) + 1L) * _subArraySize;
        long expandTo = ((_length + (long)(_length * getExpandRate())) >> _subArrayBits) * _subArraySize;
        if(capacity < expandTo) {
            capacity = expandTo;
        }
        
        // Cap length to Integer.MAX_VALUE 
        int newLength = (capacity < Integer.MAX_VALUE) ? (int)capacity : Integer.MAX_VALUE;
        
        // Reset _length
        _length = newLength;
        
        // Expand array file on disk
        _arrayFile.setArrayLength(newLength, null /* do not rename */);
        
        // Add to logging
        _logger.info("Expanded: _length=" + _length);
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(_mode == Mode.CLOSED) {
            return;
        }
        
        try {
            sync();
            _entryManager.clear();
            _arrayFile.close();
        } catch(Exception e) {
            throw (e instanceof IOException) ? (IOException)e : new IOException(e);
        } finally {
            _arrayFile = null;
            _length = 0;
            
            _mode = Mode.CLOSED;
        }
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(_mode == Mode.OPEN) {
            return;
        }
        
        File file = new File(_directory, "indexes.dat");
        _arrayFile = openArrayFile(file, _length /* initial length */, 8);
        _length = _arrayFile.getArrayLength();
        
        this.init();
        this._mode = Mode.OPEN;
        
        getLogger().info("length:" + _length +
                        " batchSize:" + _entryManager.getMaxEntrySize() +
                        " numSyncBatches:" + _entryManager.getMaxEntries() + 
                        " directory:" + _directory.getAbsolutePath() +
                        " arrayFile:" + _arrayFile.getName());
    }
    
    @Override
    public boolean isOpen() {
        return _mode == Mode.OPEN;
    }
    
    @Override
    public void updateArrayFile(List<Entry<EntryValueLong>> entryList) throws IOException {
        if(_arrayFile != null) {
            if(isOpen()) {
                _arrayFile.flush();
                
                // Find maxScn
                long maxScn = 0;
                if(entryList != null && entryList.size() > 0) {
                    for (Entry<?> e : entryList) {
                        maxScn = Math.max(e.getMaxScn(), maxScn);
                    }
                } else {
                    maxScn = this.getHWMark();
                }
                
                // update arrayFile lwmScn and hwmScn to maxScn
                if(maxScn > 0) {
                    _arrayFile.setWaterMarks(maxScn, maxScn);
                }
            } else {
                // IOTypeLongArray instantiation goes here.
                _arrayFile.update(entryList);
            }
        }
    }

    @Override
    public final Type getType() {
        return _type;
    }
}
