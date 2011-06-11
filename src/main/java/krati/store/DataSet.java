package krati.store;

import java.io.IOException;

import krati.io.Closeable;

/**
 * DataSet.
 * 
 * @author jwu
 * 
 * @param <V> value
 * 
 * <p>
 * 06/06, 2011 - Extended interface Closeable
 */
public interface DataSet<V> extends Closeable {
    
    public boolean has(V value);
    
    public boolean add(V value) throws Exception;
    
    public boolean delete(V value) throws Exception;
    
    public void sync() throws IOException;
    
    public void persist() throws IOException;
    
    public void clear() throws IOException;
}
