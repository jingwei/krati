package krati.mds;

import java.io.File;
import java.io.IOException;

/**
 * MDSLoader
 * 
 * @author jwu
 *
 */
public interface MDSLoader
{
    public void load(MDSCache mds, File dataFile) throws IOException;
    
    public void dump(MDSCache mds, File dumpFile) throws IOException;
}
