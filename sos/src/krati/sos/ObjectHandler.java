package krati.sos;

/**
 * ObjectHandler
 * 
 * @author jwu
 *
 * @param <V> the object to be handled by this handler.
 */
public interface ObjectHandler<V>
{
    public boolean getEnabled();
    public void setEnabled(boolean b);
    public boolean process(V object);
}
