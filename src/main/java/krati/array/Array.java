package krati.array;

/**
 * Array
 * 
 * @author jwu
 *
 */
public interface Array {
    
    /**
     * Clears this Array.
     */
    public void clear();
    
    /**
     * @return the current length of this Array.
     */
    public int length();
    
    /**
     * @return a boolean indicating an index is in the range of this Array.
     */
    public boolean hasIndex(int index);
    
    /**
     * Gets the type of this Array. 
     */
    public Array.Type getType();
    
    /**
     * Array.Type
     */
    public static enum Type {
        /**
         * The type of AddressArray of fixed-length.
         */
        STATIC,
        
        /**
         * The type of AddressArray of varying length.
         */
        DYNAMIC;
    }
}
