package krati.store;

import java.io.IOException;

/**
 * DataSet.
 * 
 * @author jwu
 * 
 * @param <V> value
 */
public interface DataSet<V> {
    
    public boolean has(V value);
    
    public boolean add(V value) throws Exception;
    
    public boolean delete(V value) throws Exception;
    
    public void sync() throws IOException;
    
    public void persist() throws IOException;
    
    public void clear() throws IOException;
}
