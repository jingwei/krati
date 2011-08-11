package krati.util;

import java.util.Iterator;

/**
 * IndexedIterator
 * 
 * @author  jwu
 * @since   0.4.2
 * @version 0.4.2
 * 
 * <p>
 * 08/10, 2011 - Created
 */
public interface IndexedIterator<E> extends Iterator<E> {
    
    /**
     * @return the current index.
     */
    public int index();
    
    /**
     * Resets this IndexedIterator to a new start.
     * 
     * @param indexStart - the index start
     */
    public void reset(int indexStart);
}
