package krati.core.array.basic;

import java.util.Arrays;

import krati.array.DynamicArray;
import krati.array.LongArray;

/**
 * MemoryLongArray
 * 
 * @author jwu
 * 
 */
public class MemoryLongArray implements LongArray, DynamicArray {
    protected long[][] _subArrays;
    protected final int _subArrayBits;
    protected final int _subArraySize;
    protected final int _subArrayMask;
    protected final boolean _autoExpand;
    
    public MemoryLongArray() throws Exception {
        this(16, true);
    }
    
    public MemoryLongArray(int subArrayBits) throws Exception {
        this(subArrayBits, true);
    }
    
    public MemoryLongArray(int subArrayBits, boolean autoExpand) throws Exception {
        this._subArrayBits = subArrayBits;           // e.g. 16
        this._subArraySize = 1 << subArrayBits;      // e.g. 65536
        this._subArrayMask = this._subArraySize - 1; // e.g. 65535
        this._subArrays = new long[1][_subArraySize];
        this._autoExpand = autoExpand;
    }
    
    @Override
    public void clear() {
        for (long[] subArray : _subArrays) {
            Arrays.fill(subArray, 0L);
        }
    }
    
    /**
     * @return the current length of this Array
     */
    @Override
    public int length() {
        return _subArrays.length * _subArraySize;
    }
    
    /**
     * @return a boolean indicating an index is in the current range of this Array.
     */
    @Override
    public boolean hasIndex(int index) {
        return (index < 0) ? false : (index >> _subArrayBits) < _subArrays.length;
    }
    
    @Override
    public long get(int index) {
        if (index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        
        int subInd = index >> _subArrayBits;
        int offset = index & _subArrayMask;
        
        return _subArrays[subInd][offset];
    }
    
    public void set(int index, long value) {
        if (index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        
        int subInd = index >> _subArrayBits;
        int offset = index & _subArrayMask;
        
        // Expand array capacity automatically
        if (subInd >= _subArrays.length && _autoExpand) {
            expandCapacity(index);
        }
        
        _subArrays[subInd][offset] = value;
    }
    
    @Override
    public void set(int index, long value, long scn) {
        set(index, value);
    }
    
    @Override
    public synchronized void expandCapacity(int index) {
        if (index < 0) return;
        
        int numSubArrays = (index >> _subArrayBits) + 1;
        if (numSubArrays <= _subArrays.length) {
            return; // No need to expand this array
        }
        
        long[][] tmpArrays = new long[numSubArrays][];
        
        int i = 0;
        for (; i < _subArrays.length; i++) {
            tmpArrays[i] = _subArrays[i];
        }
        
        for (; i < numSubArrays; i++) {
            tmpArrays[i] = new long[_subArraySize];
        }
        
        _subArrays = tmpArrays;
        
        if(getArrayExpandListener() != null) {
            getArrayExpandListener().arrayExpanded(this);
        }
    }
    
    @Override
    public synchronized long[] getInternalArray() {
        long[] result = new long[length()];
        for (int i = 0; i < _subArrays.length; i++) {
            System.arraycopy(_subArrays[i], 0, result, i * _subArraySize, _subArraySize);
        }
        
        return result;
    }
    
    private ArrayExpandListener _expandListener;
    
    protected void setArrayExpandListener(ArrayExpandListener listener) {
        this._expandListener = listener;
    }
    
    protected ArrayExpandListener getArrayExpandListener() {
        return _expandListener;
    }
}
