package krati.store;

import krati.Persistable;
import krati.array.DataArray;

/**
 * ArrayStore
 * 
 * @author jwu
 * 01/10, 2011
 *
 */
public interface ArrayStore extends Persistable, DataArray {
    
    /**
     * @return the capacity of this ArrayStore.
     */
    public int capacity();
    
    /**
     * @return the index start of this ArrayStore.
     */
    public int getIndexStart();
    
    /**
     * Deletes data at an index;
     * 
     * @param index       - Index where data is to be removed.
     * @param scn         - System change number.
     * @throws Exception
     */
    public void delete(int index, long scn) throws Exception;
}
