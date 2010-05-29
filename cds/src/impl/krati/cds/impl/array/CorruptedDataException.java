package krati.cds.impl.array;

/**
 * 
 * @author jwu
 *
 */
public class CorruptedDataException extends RuntimeException
{
    private static final long serialVersionUID = 1L;
    
    public CorruptedDataException(String message)
    {
        super(message);
    }
    
    public CorruptedDataException(int index)
    {
        super("Data corruption at index " + index);
    }
}
