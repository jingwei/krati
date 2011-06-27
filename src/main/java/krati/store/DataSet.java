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
    
    /**
     * @return the capacity of this DataSet.
     */
    public int capacity();
    
    /**
     * Tests if this DataSet contains a value.
     * 
     * @return <code>true</code> if the value belongs to this DataSet. Otherwise, <code>false</code>.
     * This method should always return <code>false</code> upon a <code>null</code> value.
     */
    public boolean has(V value);
    
    /**
     * Adds a value to this DataSet.
     * 
     * @return <code>true</code> if the value is added to this DataSet successfully. Otherwise, <code>false</code>.
     * This method should always return <code>false</code> upon a <code>null</code> value.
     * @throws Exception if the value cannot be added to this DataSet.
     */
    public boolean add(V value) throws Exception;
    
    /**
     * Deletes a value from this DataSet.
     * 
     * @return <code>true</code> if the value is deleted from this DataSet successfully. Otherwise, <code>false</code>.
     * This method should always return <code>false</code> upon a <code>null</code> value.
     * @throws Exception if the value cannot be deleted from this DataSet.
     */
    public boolean delete(V value) throws Exception;
    
    /**
     * Force updates to disk file in blocking mode.
     * @throws IOException
     */
    public void sync() throws IOException;
    
    /**
     * Force updates to disk files in non-blocking mode.
     * @throws IOException
     */
    public void persist() throws IOException;
    
    /**
     * Clears this DataSet by removing all values.
     * @throws IOException
     */
    public void clear() throws IOException;
}
