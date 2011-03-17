package krati.core.array.entry;

/**
 * EntryOverflowException
 * 
 * @author jwu
 * 
 */
public class EntryOverflowException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public EntryOverflowException(Entry<?> entry) {
        super("Overflow occurred on entry " + entry.getId() + " with capacity " + entry.capacity());
    }
}
