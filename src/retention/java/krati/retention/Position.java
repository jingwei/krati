package krati.retention;

import java.io.Serializable;

import krati.retention.clock.Clock;

/**
 * Position
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/01, 2011 - Created
 */
public interface Position extends Serializable {
    
    /**
     * @return the Id. 
     */
    public int getId();
    
    /**
     * @return the offset to retention.
     */
    public long getOffset();
    
    /**
     * @return the index to snapshot.
     */
    public int getIndex();
    
    /**
     * @return <tt>true</tt> if <tt>getIndex()</tt> returns a non-negative index.
     */
    public boolean isIndexed();
    
    /**
     * @return the clock associated with this Position.
     */
    public Clock getClock();
}
