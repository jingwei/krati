package krati.sos;

public class ObjectConstructionException extends RuntimeException
{
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1L;
    
    public ObjectConstructionException(String message)
    {
        super(message);
    }
    
    public ObjectConstructionException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
