package krati.io;

import java.io.IOException;

/**
 * Closeable
 * 
 * @author jwu
 * 04/21, 2011
 */
public interface Closeable extends java.io.Closeable {

    /**
     * @return <code>true</code> if the underlying service is open. Otherwise, <code>false</code>.
     */
    public boolean isOpen();
    
    /**
     * Open to start serving requests. If the service is already opened then invoking this 
     * method has no effect.
     * 
     * @throws IOException if the underlying service cannot be opened properly.
     */
    public void open() throws IOException;
    
    /**
     * Close to quit from serving requests. If the service is already closed then invoking this 
     * method has no effect.
     * 
     * @throws IOException if the underlying service cannot be closed properly.
     */
    @Override
    public void close() throws IOException;
}
