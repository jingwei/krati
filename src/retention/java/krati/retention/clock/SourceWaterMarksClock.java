package krati.retention.clock;

import java.util.Iterator;
import java.util.List;

import krati.util.SourceWaterMarks;

/**
 * SourceWaterMarksClock
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/15, 2011 - Created <br/>
 */
public class SourceWaterMarksClock implements WaterMarksClock {
    private final List<String> _sources;
    private final SourceWaterMarks _sourceWaterMarks;
    
    /**
     * Construct an instance of SourceWaterMarksClock.
     * 
     * @param sources - the list for defining the order of individual sources in a multi-source vector clock.
     * @param sourceWaterMarks - the persistent source water marks. 
     */
    public SourceWaterMarksClock(List<String> sources, SourceWaterMarks sourceWaterMarks) {
        this._sources = sources;
        this._sourceWaterMarks = sourceWaterMarks;
        
        for(String source : sources) {
            long lwm = sourceWaterMarks.getLWMScn(source);
            long hwm = sourceWaterMarks.getHWMScn(source);
            sourceWaterMarks.setWaterMarks(source, lwm, hwm);
        }
    }
    
    @Override
    public boolean hasSource(String source) {
        if(source == null) return false;
        for(String s : _sources) {
            if(s.equals(source)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * @return an iterator of sources known to this SourceWaterMarksClock.
     */
    @Override
    public Iterator<String> sourceIterator() {
        return _sources.iterator();
    }
    
    /**
     * @return the current clock of this SourceWaterMarksClock.
     */
    @Override
    public synchronized Clock current() {
        long[] values = new long[_sources.size()];
        for(int i = 0, cnt = _sources.size(); i < cnt; i++) {
            values[i] = _sourceWaterMarks.getHWMScn(_sources.get(i));
        }
        return new Clock(values);
    }
    
    /**
     * Gets the low water mark of a given source.
     * 
     * @param source
     */
    @Override
    public synchronized long getLWMScn(String source) {
        return _sourceWaterMarks.getLWMScn(source);
    }
    
    /**
     * Gets the high water mark of a given source.
     * 
     * @param source
     */
    @Override
    public synchronized long getHWMScn(String source) {
        return _sourceWaterMarks.getHWMScn(source);
    }
    
    /**
     * Sets the high water mark of a given source.
     * 
     * @param source - the data source
     * @param hwm    - the high water mark
     */
    @Override
    public synchronized void setHWMark(String source, long hwm) {
        _sourceWaterMarks.saveHWMark(source, hwm);
    }
    
    /**
     * Save the high water mark of a given source.
     * 
     * @param source
     * @param hwm
     */
    @Override
    public synchronized Clock updateHWMark(String source, long hwm) {
        _sourceWaterMarks.saveHWMark(source, hwm);
        return current();
    }
    
    @Override
    public synchronized Clock updateWaterMarks(String source, long lwm, long hwm) {
        _sourceWaterMarks.setWaterMarks(source, lwm, hwm);
        return current();
    }
    
    @Override
    public synchronized Clock syncWaterMarks(String source) {
        _sourceWaterMarks.syncWaterMarks(source);
        return current();
    }
    
    @Override
    public synchronized Clock syncWaterMarks() {
        _sourceWaterMarks.syncWaterMarks();
        return current();
    }
    
    @Override
    public synchronized boolean flush() {
        return _sourceWaterMarks.flush();
    }
    
    @Override
    public long getWaterMark(String source, Clock clock) {
        for(int i = 0, cnt = _sources.size(); i < cnt; i++) {
            if(source.equals(_sources.get(i))) {
                return clock.values()[i];
            }
        }
        
        throw new IllegalArgumentException("Unknown " + source);
    }
}
