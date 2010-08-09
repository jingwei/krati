package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import krati.cds.array.DynamicArray;
import krati.cds.array.LongArrayDual;
import krati.cds.impl.array.AddressArray;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryLongDualFactory;
import krati.cds.impl.array.entry.EntryPersistListener;
import krati.cds.impl.array.entry.EntryValueLongDual;

/**
 * DynamicLongArrayDual
 * 
 * @author jwu
 *
 */
public class DynamicLongArrayDual extends AbstractRecoverableArrayDual<EntryValueLongDual> implements AddressArray, DynamicArray, LongArrayDual, ArrayExpandListener
{
    private final static Logger _log = Logger.getLogger(DynamicLongArrayDual.class);
    private MemoryLongArray _internalArray;
    private MemoryLongArray _internalArrayDual;
    
    public DynamicLongArrayDual(int entrySize, int maxEntries, File homeDirectory) throws Exception {
        this(entrySize, maxEntries, homeDirectory, "indexes.dat", "indexes.2.dat");
    }
    
    public DynamicLongArrayDual(int entrySize, int maxEntries, File homeDirectory, String arrayFileName, String arrayFileDualName) throws Exception {
        super(DynamicConstants.SUB_ARRAY_SIZE /* initial array length and subArray length */,
              8 /* array element size */, entrySize, maxEntries, homeDirectory,
              arrayFileName, arrayFileDualName, new EntryLongDualFactory());
    }
    
    @Override
    protected void loadArrayFileData() {
        long maxScn = _arrayFile.getLwmScn();
        
        try {
            _internalArray = new MemoryLongArray(DynamicConstants.SUB_ARRAY_BITS);
            _arrayFile.load(_internalArray);
            
            expandCapacity(_internalArray.length() - 1);
            _internalArray.setArrayExpandListener(this);
        }
        catch(Exception e) {
            maxScn = 0;
            clear();
        }
        
        _entryManager.setWaterMarks(maxScn, maxScn);
    }

    @Override
    protected void loadArrayFileDualData() {
        long maxScn = _arrayFileDual.getLwmScn();
        
        try {
            _internalArrayDual = new MemoryLongArray(DynamicConstants.SUB_ARRAY_BITS);
            _arrayFileDual.load(_internalArrayDual);
            
            expandCapacity(_internalArrayDual.length() - 1);
            _internalArrayDual.setArrayExpandListener(this);
        }
        catch(Exception e) {
            maxScn = 0;
            clear();
        }
        
        _entryManager.setWaterMarks(maxScn, maxScn); // TODO
    }
    
    /**
     * Sync-up the high water mark to a given value.
     * 
     * @param endOfPeriod
     */
    @Override
    public void saveHWMark(long endOfPeriod) {
        if (getHWMark() < endOfPeriod) {
            try {
                set(0, get(0), getDual(0), endOfPeriod);
            }
            catch(Exception e) {
                _log.error("Failed to saveHWMark: " + endOfPeriod, e);
            }
        }
    }
    
    @Override
    public void clear() {
        if (_internalArray != null) {
            _internalArray.clear();
        }
        
        if (_internalArrayDual != null) {
            _internalArrayDual.clear();
        }
        
        // Clear the entry manager
        _entryManager.clear();
        
        // Clear the underly array file
        try {
            _arrayFile.reset(_internalArray, _entryManager.getLWMark());
        }
        catch(IOException e) {
            _log.error("Failed to clear _arrayFile", e);
        }

        // Clear the underly array file dual
        try {
            _arrayFileDual.reset(_internalArrayDual, _entryManager.getLWMark());
        }
        catch(IOException e) {
            _log.error("Failed to clear _arrayFileDual", e);
        }
    }
    
    @Override
    public EntryPersistListener getPersistListener() {
      return getEntryManager().getEntryPersistListener();
    }
    
    @Override
    public void setPersistListener(EntryPersistListener persistListener) {
        getEntryManager().setEntryPersistListener(persistListener);
    }
    
    @Override
    public void expandCapacity(int index) throws Exception {
        if(index < _length) return;
        
        int newLength = ((index >> DynamicConstants.SUB_ARRAY_BITS) + 1) * DynamicConstants.SUB_ARRAY_SIZE;
        
        // Reset _length
        _length = newLength;
        
        // Expand internal array in memory 
        if(_internalArray.length() < newLength) {
            _internalArray.expandCapacity(index);
        }
        if(_internalArrayDual.length() < newLength) {
            _internalArrayDual.expandCapacity(index);
        }
        
        // Expand array file on disk
        _arrayFile.setArrayLength(newLength, null /* do not rename */);
        _arrayFileDual.setArrayLength(newLength, null /* do not rename */);
        
        // Add to logging
        _log.info("Expanded: _length=" + _length);
    }
    
    @Override
    public void arrayExpanded(DynamicArray dynArray) {
        if(dynArray == _internalArray) {
            try {
                expandCapacity(dynArray.length() - 1);
            }
            catch(Exception e) {
                _log.error("Failed to expand: length=" + dynArray.length());
            }
        }
        
        if(dynArray == _internalArrayDual) {
            try {
                expandCapacity(dynArray.length() - 1);
            }
            catch(Exception e) {
                _log.error("Failed to expand: length=" + dynArray.length());
            }
        }
    }
    
    public final int subArrayLength() {
        return DynamicConstants.SUB_ARRAY_SIZE;
    }
    
    @Override
    public final long get(int index) {
        return _internalArray.get(index);
    }
    
    @Override
    public final long getDual(int index) {
        return _internalArrayDual.get(index);
    }
    
    @Override
    public final void set(int index, long value, long valueDual, long scn) throws Exception {
        _internalArray.set(index, value);
        _internalArrayDual.set(index, valueDual);
        _entryManager.addToPreFillEntryLongDual(index, value, valueDual, scn);
    }

    @Override
    public final void set(int index, long value, long scn) throws Exception {
        set(index, value, getDual(index), scn);
    }
    
    @Override
    public final void setDual(int index, long value, long scn) throws Exception {
        set(index, get(index), value, scn);
    }
    
    @Override
    public final long[] getInternalArray()
    {
        return _internalArray.getInternalArray();
    }
    
    @Override
    public final long[] getInternalArrayDual() {
        return _internalArrayDual.getInternalArray();
    }
    
    @Override
    public void updateArrayFile(List<Entry<EntryValueLongDual>> entryList) throws IOException
    {
      ArrayFile.updateDual(_arrayFile, _arrayFileDual, entryList);
    }
}
